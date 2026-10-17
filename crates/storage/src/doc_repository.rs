//! Document repository implementation for `SurrealDB`.

#![allow(clippy::items_after_statements)]

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;

use elephant_db::tokenize;
use elephant_types::records::entity::Entity;
use elephant_types::records::Doc;

use elephant_types::{Link, RecordId, TableName};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::{PaginatedResult, Repository, SearchCriteria, SearchRepository};

/// Metadata key carrying Angel protectee attribution on docs.
const ANGEL_PROTECTEES_METADATA_KEY: &str = "angel_protectees";

/// Metadata key linking a doc to the Angel monitoring session that produced it.
const ANGEL_SESSION_ID_METADATA_KEY: &str = "angel_session_id";

pub(crate) const DOC_PROJECTION: &str = r"
    id,
    space_id,
    created_at,
    updated_at,
    created_by,
    deleted_at,
    security,
    IF provenance IS NOT NONE THEN {
        sources: IF provenance.sources IS NOT NONE THEN array::map(provenance.sources, |$s| string::concat(meta::tb($s), ':', meta::id($s))) ELSE [] END,
        extractor: provenance.extractor,
        run_id: provenance.run_id,
        work_run_id: provenance.work_run_id,
        prompt_id: provenance.prompt_id,
        config_id: provenance.config_id,
        chain: provenance.chain
    } ELSE NONE END AS provenance,
    subjects,
    subjects_state,
    title,
    text,
    sidecar_text,
    source,
    media_attachments,
    rendition_key,
    metadata
";

/// Database-only proxy for `Doc` to avoid flatten/native enum issues.
#[derive(Deserialize)]
pub(crate) struct DocDb {
    id: surrealdb::sql::Thing,
    space_id: Option<String>,
    created_at: Option<chrono::DateTime<Utc>>,
    updated_at: Option<chrono::DateTime<Utc>>,
    created_by: Option<String>,
    deleted_at: Option<chrono::DateTime<Utc>>,
    provenance: Option<elephant_types::records::base::Provenance>,
    security: Option<elephant_types::records::base::Security>,
    subjects: Option<Vec<Link<Entity>>>,
    subjects_state: Option<elephant_types::records::base::SubjectsState>,
    title: Option<String>,
    text: Option<String>,
    sidecar_text: Option<String>,
    source: Option<surrealdb::sql::Thing>,
    media_attachments: Option<Vec<elephant_types::records::DocMediaAttachment>>,
    rendition_key: Option<String>,
    #[serde(default = "default_metadata")]
    metadata: serde_json::Value,
}

impl TryFrom<DocDb> for Doc {
    type Error = StorageError;

    fn try_from(db: DocDb) -> Result<Self, Self::Error> {
        Ok(Self {
            base: elephant_types::records::base::BaseFields {
                id: crate::surreal::record_id_from_thing(&db.id)
                    .map_err(|e| StorageError::QueryError(e.to_string()))?,
                space_id: db
                    .space_id
                    .ok_or_else(|| StorageError::QueryError("Missing space_id".to_string()))?,
                created_at: db
                    .created_at
                    .ok_or_else(|| StorageError::QueryError("Missing created_at".to_string()))?,
                updated_at: db
                    .updated_at
                    .ok_or_else(|| StorageError::QueryError("Missing updated_at".to_string()))?,
                created_by: db
                    .created_by
                    .ok_or_else(|| StorageError::QueryError("Missing created_by".to_string()))?,
                deleted_at: db.deleted_at,
                provenance: db
                    .provenance
                    .ok_or_else(|| StorageError::QueryError("Missing provenance".to_string()))?,
                security: db
                    .security
                    .ok_or_else(|| StorageError::QueryError("Missing security".to_string()))?,
                subjects: db
                    .subjects
                    .ok_or_else(|| StorageError::QueryError("Missing subjects".to_string()))?,
                subjects_state: db.subjects_state.ok_or_else(|| {
                    StorageError::QueryError("Missing subjects_state".to_string())
                })?,
            },
            title: db
                .title
                .ok_or_else(|| StorageError::QueryError("Missing title".to_string()))?,
            text: db
                .text
                .ok_or_else(|| StorageError::QueryError("Missing text".to_string()))?,
            sidecar_text: db.sidecar_text,
            source: db
                .source
                .map(|t| crate::surreal::record_id_from_thing(&t).map(Link::new))
                .transpose()
                .map_err(|e| StorageError::QueryError(e.to_string()))?,
            media_attachments: db.media_attachments.unwrap_or_default(),
            rendition_key: db
                .rendition_key
                .ok_or_else(|| StorageError::QueryError("Missing rendition_key".to_string()))?,
            metadata: db.metadata,
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::float_cmp,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]

    use super::*;
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::{DocMediaAttachment, DocMediaKind, DocMediaLocation};
    use elephant_types::TableName;
    use std::str::FromStr;

    fn make_doc_db() -> DocDb {
        DocDb {
            id: crate::surreal::thing_from_record_id(&RecordId::new_v4(TableName::Doc)),
            space_id: Some("space_1".to_string()),
            created_at: Some(Utc::now()),
            updated_at: Some(Utc::now()),
            created_by: Some("tester".to_string()),
            deleted_at: None,
            provenance: Some(Provenance::default()),
            security: Some(Security::default()),
            subjects: Some(Vec::new()),
            subjects_state: Some(SubjectsState::Unknown),
            title: Some("Title".to_string()),
            text: Some("Body".to_string()),
            sidecar_text: None,
            source: None,
            media_attachments: Some(Vec::new()),
            rendition_key: Some("rendition_1".to_string()),
            metadata: default_metadata(),
        }
    }

    #[test]
    fn test_doc_missing_space_id_errors() {
        let mut db = make_doc_db();
        db.space_id = None;
        let err = Doc::try_from(db).expect_err("missing space_id should error");
        assert!(matches!(err, StorageError::QueryError(msg) if msg.contains("space_id")));
    }

    #[test]
    fn test_doc_missing_created_by_errors() {
        let mut db = make_doc_db();
        db.created_by = None;
        let err = Doc::try_from(db).expect_err("missing created_by should error");
        assert!(matches!(err, StorageError::QueryError(msg) if msg.contains("created_by")));
    }

    fn test_doc(space_id: &str, title: &str, text: &str) -> Doc {
        Doc {
            base: BaseFields {
                id: RecordId::new_v4(TableName::Doc),
                space_id: space_id.to_string(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                created_by: "tester".to_string(),
                deleted_at: None,
                security: Security::default(),
                provenance: Provenance::default(),
                subjects: Vec::new(),
                subjects_state: SubjectsState::Unknown,
            },
            title: title.to_string(),
            text: text.to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: format!("rendition-{title}"),
            metadata: default_metadata(),
        }
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn assert_document_space_discovery(client: StorageClient) {
        let repo = DocRepository::new(client);
        assert!(repo
            .get_all_spaces()
            .await
            .expect("empty discovery")
            .is_empty());

        for (space, title) in [
            ("zeta", "first"),
            ("alpha", "second"),
            ("zeta", "duplicate-space"),
            ("beta", "fourth"),
        ] {
            repo.create(test_doc(space, title, "document text"))
                .await
                .expect("create document");
        }
        let deleted = repo
            .create(test_doc("deleted-only", "deleted", "retained document"))
            .await
            .expect("create document to soft-delete");
        repo.delete("deleted-only", &deleted.base.id)
            .await
            .expect("soft-delete document");

        assert_eq!(
            repo.get_all_spaces().await.expect("document spaces"),
            vec!["alpha", "beta", "deleted-only", "zeta"]
        );
    }

    #[tokio::test]
    async fn test_document_space_discovery_surreal_memory() {
        assert_document_space_discovery(StorageClient::new_memory().await.expect("memory client"))
            .await;
    }

    #[tokio::test]
    async fn test_document_space_discovery_embedded() {
        assert_document_space_discovery(
            StorageClient::new_embedded()
                .await
                .expect("embedded client"),
        )
        .await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_document_space_discovery_rejects_invalid_space_ids() {
        for sql in [
            "CREATE doc:malformed SET title = 'missing space'",
            "CREATE doc:malformed SET space_id = NULL",
            "CREATE doc:malformed SET space_id = 42",
        ] {
            let client = StorageClient::new_memory().await.expect("memory client");
            let repo = DocRepository::new(client.clone());
            repo.create(test_doc("valid", "valid", "valid document"))
                .await
                .expect("create valid document alongside malformed row");
            client.query_checked(sql).await.expect("seed malformed row");
            let error = repo
                .get_all_spaces()
                .await
                .expect_err("malformed rows must not be silently omitted");
            assert!(
                matches!(error, StorageError::DataIntegrityError(message) if message.contains("space_id")),
                "mandatory space field must be validated for {sql}"
            );
        }
    }

    fn test_doc_with_id(space_id: &str, id: &str, title: &str, text: &str) -> Doc {
        let mut doc = test_doc(space_id, title, text);
        doc.base.id = RecordId::from_str(id).expect("valid test record id");
        doc
    }

    async fn seed_angel_session_docs(repo: &DocRepository) -> RecordId {
        let mut session_doc = test_doc("default", "Session Doc", "session text");
        session_doc.metadata = serde_json::json!({
            "angel_session_id": "sess-42",
            "angel_protectees": ["child-a", "child-b"]
        });
        let session_doc_id = session_doc.base.id.clone();
        repo.create(session_doc).await.expect("create session doc");

        let mut other_doc = test_doc("default", "Other Doc", "other text");
        other_doc.metadata = serde_json::json!({ "angel_session_id": "sess-99" });
        repo.create(other_doc).await.expect("create other doc");

        repo.create(test_doc("default", "Plain Doc", "plain text"))
            .await
            .expect("create plain doc");

        session_doc_id
    }

    async fn assert_find_by_angel_session_id(repo: &DocRepository, expected_id: &RecordId) {
        let matched = repo
            .find_by_angel_session_id("default", "sess-42", 100)
            .await
            .expect("find by session id");
        assert_eq!(matched.len(), 1);
        assert_eq!(&matched[0].base.id, expected_id);

        let missing = repo
            .find_by_angel_session_id("default", "sess-unknown", 100)
            .await
            .expect("find missing session id");
        assert!(missing.is_empty());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_find_by_angel_session_id_surreal_memory() {
        let client = crate::StorageClient::new_memory()
            .await
            .expect("create in-memory surreal client");
        let repo = DocRepository::new(client);
        let session_doc_id = seed_angel_session_docs(&repo).await;
        assert_find_by_angel_session_id(&repo, &session_doc_id).await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_find_by_angel_session_id_embedded() {
        let client = crate::StorageClient::new_embedded()
            .await
            .expect("create embedded client");
        let repo = DocRepository::new(client);
        let session_doc_id = seed_angel_session_docs(&repo).await;
        assert_find_by_angel_session_id(&repo, &session_doc_id).await;
    }

    async fn assert_set_angel_protectees_replaces_exactly(repo: &DocRepository) {
        let mut doc = test_doc("default", "Attributed Doc", "text");
        doc.metadata = serde_json::json!({
            "angel_protectees": ["child-a", "child-b"],
            "note": "keep"
        });
        let doc_id = doc.base.id.clone();
        repo.create(doc).await.expect("create doc");

        // Replace, not union: child-b must be gone afterwards.
        repo.set_angel_protectees("default", &doc_id, &["child-a".to_string()])
            .await
            .expect("set protectees");
        let updated = repo
            .get("default", &doc_id)
            .await
            .expect("get doc")
            .expect("doc exists");
        assert_eq!(
            updated.metadata["angel_protectees"],
            serde_json::json!(["child-a"])
        );
        assert_eq!(updated.metadata["note"], serde_json::json!("keep"));

        // Empty list is written as an explicit empty array (family-wide).
        repo.set_angel_protectees("default", &doc_id, &[])
            .await
            .expect("set empty protectees");
        let cleared = repo
            .get("default", &doc_id)
            .await
            .expect("get doc")
            .expect("doc exists");
        assert_eq!(cleared.metadata["angel_protectees"], serde_json::json!([]));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client in the original fixture scope through assertions and cleanup."
    )]
    async fn test_set_angel_protectees_surreal_memory() {
        let client = crate::StorageClient::new_memory()
            .await
            .expect("create in-memory surreal client");
        let repo = DocRepository::new(client);
        assert_set_angel_protectees_replaces_exactly(&repo).await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client in the original fixture scope through assertions and cleanup."
    )]
    async fn test_set_angel_protectees_embedded() {
        let client = crate::StorageClient::new_embedded()
            .await
            .expect("create embedded client");
        let repo = DocRepository::new(client);
        assert_set_angel_protectees_replaces_exactly(&repo).await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_search_returns_matching_docs_in_surreal_memory() {
        let client = crate::StorageClient::new_memory()
            .await
            .expect("create in-memory surreal client");
        let repo = DocRepository::new(client);

        repo.create(test_doc(
            "default",
            "Alice Notes",
            "The white rabbit runs into wonderland.",
        ))
        .await
        .expect("create doc");

        let result = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("default")
                    .with_query("white rabbit")
                    .with_limit(10),
            )
            .await
            .expect("search docs");

        assert_eq!(result.results.len(), 1, "expected one matching document");
        assert_eq!(result.results[0].title, "Alice Notes");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_indexed_search_scopes_every_query_term() {
        let client = crate::StorageClient::new_memory()
            .await
            .expect("create in-memory surreal client");
        client
            .query(concat!(
                "DEFINE ANALYZER simple TOKENIZERS blank, class FILTERS lowercase, ascii;\n",
                include_str!("../../migrations/sql/011_doc_search.surql")
            ))
            .await
            .expect("define doc search indexes");
        let repo = DocRepository::new(client);

        repo.create(test_doc("default", "Rabbit notes", "The white rabbit."))
            .await
            .expect("create doc");
        repo.create(test_doc("other", "Gmail export", "Another space's mail."))
            .await
            .expect("create other-space doc");
        let deleted = repo
            .create(test_doc("default", "Gmail draft", "Deleted mail."))
            .await
            .expect("create deleted doc");
        repo.delete("default", &deleted.base.id)
            .await
            .expect("delete doc");

        // The space and deleted filters must hold for the second term too.
        let result = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("default")
                    .with_query("rabbit gmail")
                    .with_limit(10),
            )
            .await
            .expect("search docs");

        let titles: Vec<&str> = result
            .results
            .iter()
            .map(|doc| doc.title.as_str())
            .collect();
        assert_eq!(titles, vec!["Rabbit notes"]);
        assert_eq!(result.total, 1);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_surreal_memory_roundtrip_preserves_media_attachments() {
        let client = crate::StorageClient::new_memory()
            .await
            .expect("create in-memory surreal client");
        let repo = DocRepository::new(client);

        let mut doc = test_doc("default", "Image Evidence", "");
        doc.sidecar_text = Some("Observed screenshot".to_string());
        doc.media_attachments = vec![DocMediaAttachment {
            attachment_id: "att-1".to_string(),
            kind: DocMediaKind::Image,
            media_type: "image/jpeg".to_string(),
            location: DocMediaLocation::Path {
                path: "/tmp/elephant-example-media/frames/example.jpg".to_string(),
            },
            label: Some("example.jpg".to_string()),
            size_bytes: Some(1_024),
            duration_ms: None,
            sha256: Some("deadbeef".repeat(8)),
        }];

        let created = repo
            .create(doc.clone())
            .await
            .expect("create media-backed doc");
        assert_eq!(
            created.media_attachments, doc.media_attachments,
            "create roundtrip should preserve media attachments"
        );
        assert_eq!(
            created.sidecar_text, doc.sidecar_text,
            "create roundtrip should preserve sidecar text"
        );

        let loaded = repo
            .get("default", &doc.base.id)
            .await
            .expect("load created doc")
            .expect("created doc should exist");
        assert_eq!(
            loaded.media_attachments, doc.media_attachments,
            "get roundtrip should preserve media attachments"
        );
        assert_eq!(
            loaded.sidecar_text, doc.sidecar_text,
            "get roundtrip should preserve sidecar text"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_ranked_search_cursor_uses_offset_pagination() {
        let client = crate::StorageClient::new_memory()
            .await
            .expect("create in-memory surreal client");
        let repo = DocRepository::new(client);

        repo.create(test_doc_with_id(
            "default",
            "doc:ffffffff-ffff-ffff-ffff-ffffffffffff",
            "High Score",
            "white rabbit wonderland white rabbit",
        ))
        .await
        .expect("create high score doc");
        repo.create(test_doc_with_id(
            "default",
            "doc:00000000-0000-0000-0000-000000000001",
            "Mid Score",
            "white rabbit wonderland",
        ))
        .await
        .expect("create mid score doc");
        repo.create(test_doc_with_id(
            "default",
            "doc:00000000-0000-0000-0000-000000000002",
            "Low Score",
            "white rabbit",
        ))
        .await
        .expect("create low score doc");

        let full = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("default")
                    .with_query("white rabbit wonderland")
                    .with_limit(10),
            )
            .await
            .expect("full ranked search");
        assert_eq!(full.results.len(), 3);

        let page_one = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("default")
                    .with_query("white rabbit wonderland")
                    .with_limit(1),
            )
            .await
            .expect("first page");

        assert_eq!(page_one.results.len(), 1);
        assert_eq!(page_one.results[0].base.id, full.results[0].base.id);
        assert_eq!(page_one.cursor.as_deref(), Some("1"));

        let page_two = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("default")
                    .with_query("white rabbit wonderland")
                    .with_limit(1)
                    .with_cursor("1"),
            )
            .await
            .expect("second page");

        assert_eq!(page_two.results.len(), 1);
        assert_eq!(page_two.results[0].base.id, full.results[1].base.id);
        assert_ne!(page_one.results[0].base.id, page_two.results[0].base.id);
        assert_eq!(page_two.cursor.as_deref(), Some("2"));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_embedded_ranked_search_last_page_has_more_false() {
        let client = crate::StorageClient::new_embedded()
            .await
            .expect("create embedded client");
        let repo = DocRepository::new(client);

        repo.create(test_doc_with_id(
            "default",
            "doc:ffffffff-ffff-ffff-ffff-ffffffffffff",
            "High Score",
            "white rabbit wonderland white rabbit",
        ))
        .await
        .expect("create high score doc");
        repo.create(test_doc_with_id(
            "default",
            "doc:00000000-0000-0000-0000-000000000001",
            "Mid Score",
            "white rabbit wonderland",
        ))
        .await
        .expect("create mid score doc");

        let page_one = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("default")
                    .with_query("white rabbit wonderland")
                    .with_limit(1),
            )
            .await
            .expect("first page");
        assert!(page_one.has_more);
        assert_eq!(page_one.cursor.as_deref(), Some("1"));

        let page_two = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("default")
                    .with_query("white rabbit wonderland")
                    .with_limit(1)
                    .with_cursor("1"),
            )
            .await
            .expect("second page");
        assert_eq!(page_two.results.len(), 1);
        assert!(!page_two.has_more);
        assert!(page_two.cursor.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_embedded_ranked_search_zero_limit_has_no_cursor() {
        let client = crate::StorageClient::new_embedded()
            .await
            .expect("create embedded client");
        let repo = DocRepository::new(client);

        repo.create(test_doc_with_id(
            "default",
            "doc:ffffffff-ffff-ffff-ffff-ffffffffffff",
            "High Score",
            "white rabbit wonderland white rabbit",
        ))
        .await
        .expect("create high score doc");

        let page = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("default")
                    .with_query("white rabbit wonderland")
                    .with_limit(0),
            )
            .await
            .expect("zero-limit page");

        assert!(page.results.is_empty());
        assert!(!page.has_more);
        assert!(page.cursor.is_none());
        assert_eq!(page.total, 1);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_embedded_update_uses_target_id_not_payload_id() {
        let client = crate::StorageClient::new_embedded()
            .await
            .expect("create embedded client");
        let repo = DocRepository::new(client);

        let target = test_doc_with_id(
            "default",
            "doc:00000000-0000-0000-0000-000000000001",
            "Target",
            "Original target body",
        );
        let other = test_doc_with_id(
            "default",
            "doc:00000000-0000-0000-0000-000000000002",
            "Other",
            "Original other body",
        );

        let target_id = target.base.id.clone();
        let other_id = other.base.id.clone();

        repo.create(target).await.expect("create target doc");
        repo.create(other.clone()).await.expect("create other doc");

        let mut payload = other;
        payload.rendition_key = "rendition-Target".to_string();
        payload.title = "Updated through target".to_string();
        payload.text = "Updated target body".to_string();

        let updated = repo
            .update("default", &target_id, payload)
            .await
            .expect("update target doc");

        assert_eq!(updated.base.id, target_id);
        assert_eq!(updated.title, "Updated through target");
        assert_eq!(updated.text, "Updated target body");

        let target_after = repo
            .get("default", &target_id)
            .await
            .expect("get target doc")
            .expect("target doc exists");
        assert_eq!(target_after.title, "Updated through target");
        assert_eq!(target_after.text, "Updated target body");

        let other_after = repo
            .get("default", &other_id)
            .await
            .expect("get other doc")
            .expect("other doc exists");
        assert_eq!(other_after.title, "Other");
        assert_eq!(other_after.text, "Original other body");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_embedded_create_rejects_duplicate_rendition_key() {
        let client = crate::StorageClient::new_embedded()
            .await
            .expect("create embedded client");
        let repo = DocRepository::new(client);

        let first = test_doc("default", "First", "First body");
        let mut second = test_doc("default", "Second", "Second body");
        second.rendition_key.clone_from(&first.rendition_key);

        repo.create(first).await.expect("create first doc");
        let err = repo
            .create(second)
            .await
            .expect_err("duplicate rendition_key should conflict");
        assert!(
            matches!(err, StorageError::Conflict(msg) if msg.contains("Duplicate doc rendition_key"))
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_embedded_update_rejects_duplicate_rendition_key() {
        let client = crate::StorageClient::new_embedded()
            .await
            .expect("create embedded client");
        let repo = DocRepository::new(client);

        let first = test_doc("default", "First", "First body");
        let second = test_doc("default", "Second", "Second body");
        let second_id = second.base.id.clone();

        let first = repo.create(first).await.expect("create first doc");
        let second = repo.create(second).await.expect("create second doc");

        let mut updated = second.clone();
        updated.rendition_key.clone_from(&first.rendition_key);

        let err = repo
            .update("default", &second_id, updated)
            .await
            .expect_err("duplicate rendition_key should conflict");
        assert!(
            matches!(err, StorageError::Conflict(msg) if msg.contains("Duplicate doc rendition_key"))
        );

        let second_after = repo
            .get("default", &second.base.id)
            .await
            .expect("load second doc")
            .expect("second doc exists");
        assert_ne!(second_after.rendition_key, first.rendition_key);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_embedded_create_rejects_reusing_soft_deleted_rendition_key() {
        let client = crate::StorageClient::new_embedded()
            .await
            .expect("create embedded client");
        let repo = DocRepository::new(client);

        let original = test_doc_with_id(
            "default",
            "doc:00000000-0000-0000-0000-000000000010",
            "Original",
            "Original body",
        );
        let original_id = original.base.id.clone();
        let rendition_key = original.rendition_key.clone();

        repo.create(original).await.expect("create original doc");
        repo.delete("default", &original_id)
            .await
            .expect("soft delete original doc");

        let mut replacement = test_doc_with_id(
            "default",
            "doc:00000000-0000-0000-0000-000000000011",
            "Replacement",
            "Replacement body",
        );
        replacement.rendition_key = rendition_key;

        let err = repo
            .create(replacement)
            .await
            .expect_err("soft-deleted rendition_key should still conflict");
        assert!(
            matches!(err, StorageError::Conflict(msg) if msg.contains("Duplicate doc rendition_key"))
        );
    }

    /// Guarded restore acts only on the snapshot the caller was authorized on.
    async fn assert_restore_guarded_lifecycle(repo: &DocRepository, space: &str, record: Doc) {
        let id = record.base.id.clone();
        repo.create(record).await.unwrap();

        // A fresh tombstone snapshot restores.
        repo.delete(space, &id).await.unwrap();
        let tombstone = repo.get_with_deleted(space, &id).await.unwrap().unwrap();
        let restored = repo.restore_guarded(space, &id, &tombstone).await.unwrap();
        assert!(restored.base.deleted_at.is_none());

        // Tombstone ABA: snapshot D1, restored and deleted again as D2; the
        // stale restore from D1 conflicts and the row stays deleted.
        repo.delete(space, &id).await.unwrap();
        let tombstone_d1 = repo.get_with_deleted(space, &id).await.unwrap().unwrap();
        repo.restore(space, &id).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        repo.delete(space, &id).await.unwrap();
        let result = repo.restore_guarded(space, &id, &tombstone_d1).await;
        assert!(
            matches!(result, Err(StorageError::Conflict(_))),
            "{result:?}"
        );
        assert!(repo.get(space, &id).await.unwrap().is_none());

        // Another space never matches.
        let tombstone_d2 = repo.get_with_deleted(space, &id).await.unwrap().unwrap();
        let result = repo
            .restore_guarded("other_space", &id, &tombstone_d2)
            .await;
        assert!(
            matches!(result, Err(StorageError::Conflict(_))),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn test_restore_guarded_lifecycle_surreal() {
        let repo = DocRepository::new(crate::StorageClient::new_memory().await.unwrap());
        assert_restore_guarded_lifecycle(
            &repo,
            "default",
            test_doc("default", "Guarded", "Guarded text"),
        )
        .await;
    }

    #[tokio::test]
    async fn test_restore_guarded_lifecycle_embedded() {
        let repo = DocRepository::new(crate::StorageClient::new_embedded().await.unwrap());
        assert_restore_guarded_lifecycle(
            &repo,
            "default",
            test_doc("default", "Guarded", "Guarded text"),
        )
        .await;
    }
}

fn default_metadata() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

/// Document repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct DocRepository {
    client: StorageClient,
}

impl DocRepository {
    /// Tests exact physical document presence across spaces, including deletion.
    ///
    /// This boolean grants no read permission and exposes no foreign payload.
    /// Use it when a scoped missing row must not become an absence fallback.
    ///
    /// # Errors
    /// Refuses a non-Doc identity or malformed physical storage response.
    pub async fn exists_globally(&self, id: &RecordId) -> StorageResult<bool> {
        if id.table() != TableName::Doc {
            return Err(StorageError::DataIntegrityError(
                "document presence requires Doc identity".into(),
            ));
        }
        if let Some(store) = self.client.embedded_store() {
            return store
                .get_by_id::<Doc>(id, true)
                .await
                .map(|row| row.is_some());
        }
        #[derive(Deserialize)]
        struct Presence {
            id: RecordId,
        }
        let mut response = self
            .client
            .query_bind_checked(
                "SELECT id FROM $id;",
                crate::bindings! {"id" => crate::surreal::thing_from_record_id(id)},
            )
            .await?;
        let rows: Vec<Presence> = response
            .take(0)
            .map_err(|error| StorageError::DataIntegrityError(error.to_string()))?;
        if rows.len() > 1 || rows.iter().any(|row| row.id != *id) {
            return Err(StorageError::DataIntegrityError(
                "document presence changed physical identity".into(),
            ));
        }
        Ok(!rows.is_empty())
    }

    /// Restores a document only if it is unchanged since `authorized`, the row
    /// the caller's authorization was decided on.
    ///
    /// See [`crate::lifecycle_guard`]: the stored security, subjects, subjects
    /// state and exact lifecycle must still equal `authorized` in the same
    /// mutation that restores.
    ///
    /// # Errors
    ///
    /// Returns a conflict if the row is missing or changed, or an error if
    /// storage access fails.
    pub async fn restore_guarded(
        &self,
        space_id: &str,
        id: &RecordId,
        authorized: &Doc,
    ) -> StorageResult<Doc> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let Some(mut current) = mutation.get::<Doc>(space_id, id, true)? else {
                        return Err(crate::lifecycle_guard::guard_conflict("Doc", id));
                    };
                    if !crate::lifecycle_guard::base_envelope_unchanged(
                        &current.base,
                        &authorized.base,
                    ) {
                        return Err(crate::lifecycle_guard::guard_conflict("Doc", id));
                    }
                    current.base.deleted_at = None;
                    current.base.updated_at = chrono::Utc::now();
                    Ok(mutation.replace(current)?)
                })
                .await;
        }

        let (condition, mut bindings) =
            crate::lifecycle_guard::base_envelope_condition(&authorized.base)?;
        bindings.insert(
            "id".to_string(),
            surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(id)),
        );
        bindings.insert(
            "space_id".to_string(),
            surrealdb::sql::Value::from(space_id),
        );
        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id SET deleted_at = NONE, updated_at = time::now() \
                     WHERE space_id = $space_id AND {condition} RETURN {DOC_PROJECTION}"
                ),
                bindings,
            )
            .await?;
        let rows: Vec<DocDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse guarded document restore: {e}"))
        })?;
        Doc::try_from(
            rows.into_iter()
                .next()
                .ok_or_else(|| crate::lifecycle_guard::guard_conflict("Doc", id))?,
        )
    }

    /// Creates a new document repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Hard deletes a document (permanent).
    ///
    /// # Errors
    ///
    /// Returns an error if the delete query fails.
    pub async fn hard_delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.hard_delete::<Doc>(space_id, id).await;
        }

        self.client
            .query_bind_checked(
                "DELETE FROM $id WHERE space_id = $space_id",
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        Ok(())
    }

    /// Updates the canonical rendered text projection for a document.
    ///
    /// # Errors
    ///
    /// Returns an error if the document does not exist or the update fails.
    pub async fn update_render_projection(
        &self,
        space_id: &str,
        id: &RecordId,
        text: &str,
    ) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            let target_space_id = space_id.to_string();
            let target_id = id.clone();
            let next_text = text.to_string();
            return store
                .mutate(|mutation| {
                    let Some(mut existing) =
                        mutation.get::<Doc>(&target_space_id, &target_id, false)?
                    else {
                        return Err(StorageError::NotFound(target_id.to_string()));
                    };
                    existing.text.clone_from(&next_text);
                    existing.base.updated_at = Utc::now();
                    Ok(mutation.replace(existing)?)
                })
                .await
                .map(|_: Doc| ());
        }

        let response = self
            .client
            .query_bind(
                "UPDATE $id SET text = $text, updated_at = time::now() WHERE space_id = $space_id AND deleted_at IS NONE;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "text" => text,
                },
            )
            .await
            .map_err(|e| {
                StorageError::classify_query_error(format!(
                    "Failed to update doc render projection: {e}"
                ))
            })?;
        response.check().map_err(|e| {
            StorageError::classify_query_error(format!(
                "Failed to update doc render projection: {e}"
            ))
        })?;

        self.get(space_id, id)
            .await?
            .map(|_| ())
            .ok_or_else(|| StorageError::NotFound(id.to_string()))
    }

    /// Finds documents by rendition key.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn find_by_rendition_key(
        &self,
        space_id: &str,
        rendition_key: &str,
    ) -> StorageResult<Option<Doc>> {
        if let Some(store) = self.client.embedded_store() {
            return Ok(store.scan::<Doc>().await?.into_iter().find(|doc| {
                doc.base.space_id == space_id
                    && doc.base.deleted_at.is_none()
                    && doc.rendition_key == rendition_key
            }));
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {DOC_PROJECTION} FROM doc WHERE space_id = $space_id AND rendition_key = $rendition_key AND deleted_at IS NONE"
                ),
                crate::bindings! { "space_id" => space_id, "rendition_key" => rendition_key },
            )
            .await?;

        let docs: Vec<DocDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse doc: {e}")))?;

        docs.into_iter().next().map(Doc::try_from).transpose()
    }

    /// Finds documents that haven't been chunked yet.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn find_pending_ingestion(
        &self,
        space_id: &str,
        limit: usize,
    ) -> StorageResult<Vec<Doc>> {
        if let Some(store) = self.client.embedded_store() {
            let revision_doc_ids: std::collections::HashSet<_> = store
                .scan::<elephant_types::records::DocRevision>()
                .await?
                .into_iter()
                .filter(|revision| revision.base.deleted_at.is_none())
                .map(|revision| revision.doc.id().clone())
                .collect();

            let mut docs: Vec<_> = store
                .scan::<Doc>()
                .await?
                .into_iter()
                .filter(|doc| {
                    doc.base.space_id == space_id
                        && doc.base.deleted_at.is_none()
                        && !revision_doc_ids.contains(&doc.base.id)
                })
                .collect();
            if docs.len() > limit {
                docs.truncate(limit);
            }
            return Ok(docs);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {DOC_PROJECTION} FROM doc WHERE space_id = $space_id AND deleted_at IS NONE AND count(->doc_chunk) = 0 LIMIT {limit}"
                ),
                crate::bindings! { "space_id" => space_id },
            )
            .await?;

        let docs: Vec<DocDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse docs: {e}")))?;

        docs.into_iter().map(Doc::try_from).collect()
    }

    /// Finds documents by source ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn find_by_source(
        &self,
        space_id: &str,
        source_id: &RecordId,
        limit: usize,
    ) -> StorageResult<Vec<Doc>> {
        if let Some(store) = self.client.embedded_store() {
            let mut docs: Vec<_> = store
                .scan::<Doc>()
                .await?
                .into_iter()
                .filter(|doc| {
                    doc.base.space_id == space_id
                        && doc.base.deleted_at.is_none()
                        && doc
                            .source
                            .as_ref()
                            .is_some_and(|source| source.id() == source_id)
                })
                .collect();
            docs.sort_by_key(|doc| std::cmp::Reverse(doc.base.created_at));
            if docs.len() > limit {
                docs.truncate(limit);
            }
            return Ok(docs);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {DOC_PROJECTION} FROM doc \
                     WHERE space_id = $space_id \
                     AND source = $source_id \
                     AND deleted_at IS NONE \
                     ORDER BY created_at DESC \
                     LIMIT {limit}"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "source_id" => crate::surreal::thing_from_record_id(source_id)
                },
            )
            .await?;

        let docs: Vec<DocDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse docs: {e}")))?;

        docs.into_iter().map(Doc::try_from).collect()
    }

    /// Finds documents whose `metadata.angel_session_id` equals the given
    /// session id (Angel's per-monitoring-session doc linkage).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn find_by_angel_session_id(
        &self,
        space_id: &str,
        session_id: &str,
        limit: usize,
    ) -> StorageResult<Vec<Doc>> {
        if let Some(store) = self.client.embedded_store() {
            let mut docs: Vec<_> = store
                .scan::<Doc>()
                .await?
                .into_iter()
                .filter(|doc| {
                    doc.base.space_id == space_id
                        && doc.base.deleted_at.is_none()
                        && doc
                            .metadata
                            .get(ANGEL_SESSION_ID_METADATA_KEY)
                            .and_then(serde_json::Value::as_str)
                            == Some(session_id)
                })
                .collect();
            docs.sort_by_key(|doc| std::cmp::Reverse(doc.base.created_at));
            if docs.len() > limit {
                docs.truncate(limit);
            }
            return Ok(docs);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {DOC_PROJECTION} FROM doc \
                     WHERE space_id = $space_id \
                     AND deleted_at IS NONE \
                     AND metadata.{ANGEL_SESSION_ID_METADATA_KEY} = $session_id \
                     ORDER BY created_at DESC \
                     LIMIT {limit}"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "session_id" => session_id
                },
            )
            .await?;

        let docs: Vec<DocDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse docs: {e}")))?;

        docs.into_iter().map(Doc::try_from).collect()
    }

    /// Replaces `metadata.angel_protectees` on a document with exactly the
    /// given list (attribution corrections are authoritative — no union).
    ///
    /// An empty list is written as an explicit empty array, marking the doc
    /// family-wide while recording that the attribution was corrected.
    ///
    /// # Errors
    ///
    /// Returns an error if the document does not exist or the update fails.
    pub async fn set_angel_protectees(
        &self,
        space_id: &str,
        id: &RecordId,
        protectees: &[String],
    ) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            let target_space_id = space_id.to_string();
            let target_id = id.clone();
            let values = protectees.to_vec();
            return store
                .mutate(|mutation| {
                    let Some(mut existing) =
                        mutation.get::<Doc>(&target_space_id, &target_id, false)?
                    else {
                        return Err(StorageError::NotFound(target_id.to_string()));
                    };
                    if !existing.metadata.is_object() {
                        existing.metadata = serde_json::Value::Object(serde_json::Map::new());
                    }
                    if let Some(map) = existing.metadata.as_object_mut() {
                        map.insert(
                            ANGEL_PROTECTEES_METADATA_KEY.to_string(),
                            serde_json::Value::Array(
                                values
                                    .iter()
                                    .cloned()
                                    .map(serde_json::Value::String)
                                    .collect(),
                            ),
                        );
                    }
                    existing.base.updated_at = Utc::now();
                    Ok(mutation.replace(existing)?)
                })
                .await
                .map(|_: Doc| ());
        }

        let response = self
            .client
            .query_bind(
                &format!(
                    "UPDATE $id SET metadata.{ANGEL_PROTECTEES_METADATA_KEY} = $values, updated_at = time::now() \
                     WHERE space_id = $space_id AND deleted_at IS NONE;"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "values" => surrealdb::sql::Value::from(protectees.to_vec()),
                },
            )
            .await
            .map_err(|e| {
                StorageError::classify_query_error(format!(
                    "Failed to set doc angel_protectees: {e}"
                ))
            })?;
        response.check().map_err(|e| {
            StorageError::classify_query_error(format!("Failed to set doc angel_protectees: {e}"))
        })?;

        self.get(space_id, id)
            .await?
            .map(|_| ())
            .ok_or_else(|| StorageError::NotFound(id.to_string()))
    }

    /// Gets a document by ID, including deleted ones.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<Doc>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<Doc>(space_id, id, true).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!("SELECT {DOC_PROJECTION} FROM doc WHERE id = $id AND space_id = $space_id"),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let docs: Vec<DocDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse doc: {e}")))?;

        docs.into_iter().next().map(Doc::try_from).transpose()
    }

    /// Gets multiple documents by ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn get_by_ids(
        &self,
        space_id: &str,
        ids: &[RecordId],
        include_deleted: bool,
    ) -> StorageResult<Vec<Doc>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get_many(space_id, ids, include_deleted).await;
        }

        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let deleted_clause = if include_deleted {
            ""
        } else {
            " AND deleted_at IS NONE"
        };
        // FROM $ids fetches the listed records directly; `FROM doc WHERE id
        // INSIDE $ids` scanned the whole table.
        let query =
            format!("SELECT {DOC_PROJECTION} FROM $ids WHERE space_id = $space_id{deleted_clause}");
        let mut response = self
            .client
            .query_bind(
                &query,
                crate::bindings! {
                    "ids" => surrealdb::sql::Value::Array(surrealdb::sql::Array::from(
                        ids
                            .iter()
                            .map(crate::surreal::thing_from_record_id)
                            .map(surrealdb::sql::Value::from)
                            .collect::<Vec<_>>(),
                    )),
                    "space_id" => space_id,
                },
            )
            .await?;

        let rows: Vec<DocDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse docs: {e}")))?;
        let mut docs_by_id = std::collections::HashMap::with_capacity(rows.len());
        for row in rows {
            let doc = Doc::try_from(row)?;
            let _ = docs_by_id.insert(doc.base.id.clone(), doc);
        }

        Ok(ids.iter().filter_map(|id| docs_by_id.remove(id)).collect())
    }

    /// Restores a soft-deleted document.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails or the document is not found.
    pub async fn restore(&self, space_id: &str, id: &RecordId) -> StorageResult<Doc> {
        if let Some(store) = self.client.embedded_store() {
            return store.restore::<Doc>(space_id, id).await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id SET deleted_at = NONE, updated_at = time::now() WHERE space_id = $space_id RETURN {DOC_PROJECTION}"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let docs: Vec<DocDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse restored doc: {e}")))?;

        let doc_db = docs
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(format!("Doc not found after restore: {id}")))?;

        Doc::try_from(doc_db)
    }

    /// Replaces an existing document and clears any soft-delete flag in one
    /// repository operation.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails or the document is not found.
    pub async fn restore_and_replace_existing(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: Doc,
    ) -> StorageResult<Doc> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .restore_and_replace_existing(space_id, id, record)
                .await;
        }

        record.base.id.clone_from(id);
        record.base.space_id = space_id.to_string();
        record.base.deleted_at = None;

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id REPLACE $record WHERE space_id = $space_id RETURN {DOC_PROJECTION};"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => crate::surreal::doc_to_value(&record)?,
                },
            )
            .await?;

        let docs: Vec<DocDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse restored-and-replaced doc: {e}"))
        })?;

        let doc_db = docs.into_iter().next().ok_or_else(|| {
            StorageError::NotFound(format!("Doc not found after restore-and-replace: {id}"))
        })?;

        Doc::try_from(doc_db)
    }

    /// Replaces an existing document with the exact provided snapshot, even if
    /// that snapshot remains soft-deleted.
    pub async fn replace_existing_snapshot(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: Doc,
    ) -> StorageResult<Doc> {
        if let Some(store) = self.client.embedded_store() {
            return store.replace_existing_any_state(space_id, id, record).await;
        }

        record.base.id.clone_from(id);
        record.base.space_id = space_id.to_string();

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id REPLACE $record WHERE space_id = $space_id RETURN {DOC_PROJECTION};"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => crate::surreal::doc_to_value(&record)?,
                },
            )
            .await?;

        let docs: Vec<DocDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse snapshot-replaced doc: {e}"))
        })?;

        let doc_db = docs.into_iter().next().ok_or_else(|| {
            StorageError::NotFound(format!("Doc not found after snapshot replace: {id}"))
        })?;

        Doc::try_from(doc_db)
    }

    /// Gets distinct collections for a space.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn get_distinct_collections_for_space(
        &self,
        space_id: &str,
    ) -> StorageResult<Vec<String>> {
        if let Some(store) = self.client.embedded_store() {
            let mut collections: Vec<_> = store
                .scan::<elephant_types::records::DocNode>()
                .await?
                .into_iter()
                .filter(|node| node.base.space_id == space_id && node.base.deleted_at.is_none())
                .filter_map(|node| node.collection)
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            collections.sort();
            return Ok(collections);
        }

        let mut response = self
            .client
            .query_bind(
                "SELECT array::distinct(collection) FROM doc_chunk WHERE space_id = $space_id GROUP ALL",
                crate::bindings! { "space_id" => space_id },
            )
            .await?;

        let collections: Vec<serde_json::Value> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to get collections: {e}")))?;

        let mut results = Vec::new();
        for value in collections {
            match value {
                serde_json::Value::Array(items) => {
                    for item in items {
                        if let Some(s) = item.as_str() {
                            results.push(s.to_string());
                        }
                    }
                }
                serde_json::Value::Object(map) => {
                    if let Some(serde_json::Value::Array(items)) = map.get("array::distinct") {
                        for item in items {
                            if let Some(s) = item.as_str() {
                                results.push(s.to_string());
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(results)
    }

    /// Gets sorted unique space IDs from stored documents, including soft-deleted docs.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or a stored space ID cannot be decoded.
    /// Missing or invalid mandatory space IDs are data-integrity errors.
    pub async fn get_all_spaces(&self) -> StorageResult<Vec<String>> {
        if let Some(store) = self.client.embedded_store() {
            let mut spaces: Vec<_> = store
                .scan::<Doc>()
                .await?
                .into_iter()
                .map(|doc| doc.base.space_id)
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            spaces.sort();
            return Ok(spaces);
        }

        // Grouped object rows keep the mandatory field explicit at the DB
        // boundary. The aggregate VALUE projection is not a nested string array
        // in SurrealDB and prevents the background embed loop discovering spaces.
        #[derive(Deserialize)]
        struct SpaceRow {
            space_id: String,
        }

        let mut response = self
            .client
            .query_checked("SELECT space_id FROM doc GROUP BY space_id")
            .await?;

        let rows: Vec<SpaceRow> = response.take(0).map_err(|error| {
            StorageError::DataIntegrityError(format!(
                "Invalid document space_id during space discovery: {error}"
            ))
        })?;
        let mut spaces: Vec<_> = rows.into_iter().map(|row| row.space_id).collect();
        spaces.sort();
        spaces.dedup();
        Ok(spaces)
    }
}

#[async_trait]
impl Repository<Doc> for DocRepository {
    async fn create(&self, record: Doc) -> StorageResult<Doc> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let duplicate = mutation.scan::<Doc>()?.into_iter().any(|doc| {
                        doc.base.space_id == record.base.space_id
                            && doc.rendition_key == record.rendition_key
                    });
                    if duplicate {
                        return Err(StorageError::Conflict(format!(
                            "Duplicate doc rendition_key for space {}: {}",
                            record.base.space_id, record.rendition_key
                        )));
                    }
                    Ok(mutation.create(record.clone())?)
                })
                .await;
        }

        let create_response = self
            .client
            .query_bind(
                "CREATE $id CONTENT $record;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => crate::surreal::doc_to_value(&record)?,
                },
            )
            .await
            .map_err(|e| {
                StorageError::classify_query_error(format!("Failed to create doc: {e}"))
            })?;
        create_response.check().map_err(|e| {
            StorageError::classify_query_error(format!("Failed to create doc: {e}"))
        })?;

        let mut response = self
            .client
            .query_bind(
                &format!("SELECT {DOC_PROJECTION} FROM doc WHERE id = $id"),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(&record.base.id) },
            )
            .await?;

        let docs: Vec<DocDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse created doc: {e}")))?;

        docs.into_iter()
            .next()
            .map_or_else(|| Ok(record), Doc::try_from)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Doc>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<Doc>(space_id, id, false).await;
        }

        // Record access (`FROM ONLY $id`): the planner has no id-equality
        // fast path, so the `FROM doc WHERE id = $id` form scanned the whole
        // table on each point lookup (issue #34). The space_id/deleted_at
        // WHERE clause preserves space isolation.
        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {DOC_PROJECTION} FROM ONLY $id WHERE space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let doc: Option<DocDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse doc: {e}")))?;

        doc.map(Doc::try_from).transpose()
    }

    async fn update(&self, space_id: &str, id: &RecordId, record: Doc) -> StorageResult<Doc> {
        let target_space_id = space_id.to_string();
        let target_id = id.clone();
        let mut normalized = record;
        normalized.base.space_id.clone_from(&target_space_id);
        normalized.base.id.clone_from(&target_id);

        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let Some(_existing) =
                        mutation.get::<Doc>(&target_space_id, &target_id, false)?
                    else {
                        return Err(StorageError::NotFound(target_id.to_string()));
                    };

                    let duplicate = mutation.scan::<Doc>()?.into_iter().any(|doc| {
                        doc.base.id != target_id
                            && doc.base.space_id == target_space_id
                            && doc.rendition_key == normalized.rendition_key
                    });
                    if duplicate {
                        return Err(StorageError::Conflict(format!(
                            "Duplicate doc rendition_key for space {}: {}",
                            target_space_id, normalized.rendition_key
                        )));
                    }

                    Ok(mutation.replace(normalized.clone())?)
                })
                .await;
        }

        let update_response = self
            .client
            .query_bind(
                "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => crate::surreal::doc_to_value(&normalized)?,
                },
            )
            .await
            .map_err(|e| {
                StorageError::classify_query_error(format!("Failed to update doc: {e}"))
            })?;
        update_response.check().map_err(|e| {
            StorageError::classify_query_error(format!("Failed to update doc: {e}"))
        })?;

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {DOC_PROJECTION} FROM doc WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                },
            )
            .await?;

        let docs: Vec<DocDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse updated doc: {e}")))?;

        let doc_db = docs
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(format!("Doc not found in space: {id}")))?;

        Doc::try_from(doc_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<Doc>(space_id, id).await;
        }

        self.client
            .query_bind_checked(
                "UPDATE $id SET deleted_at = time::now(), updated_at = time::now() WHERE space_id = $space_id",
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        Ok(())
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<Doc>> {
        if let Some(store) = self.client.embedded_store() {
            return store.list::<Doc>(space_id, limit, cursor, false).await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {DOC_PROJECTION} FROM doc WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {DOC_PROJECTION} FROM doc WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
            )
        };

        let mut response = self
            .client
            .query_bind(
                &query,
                crate::bindings! {
                    "cursor" => cursor.map_or(surrealdb::sql::Value::None, |r| {
                        surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(r))
                    }),
                    "space_id" => space_id
                },
            )
            .await?;

        let docs: Vec<DocDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse docs: {e}")))?;

        docs.into_iter().map(Doc::try_from).collect()
    }
}

#[allow(clippy::too_many_lines)]
#[async_trait]
impl SearchRepository<Doc> for DocRepository {
    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Doc>> {
        if let Some(store) = self.client.embedded_store() {
            if let Some(space_id) = &criteria.space_id {
                if criteria.query.is_some() {
                    let offset = ranked_search_offset(criteria.cursor.as_deref())?;
                    if criteria.limit == 0 {
                        let (_, total) = store
                            .search_doc_ids_page(
                                space_id,
                                criteria.query.as_deref(),
                                criteria.include_deleted,
                                None,
                                0,
                            )
                            .await?;
                        return Ok(PaginatedResult::new(Vec::new(), None, false, total));
                    }

                    let fetch_limit = offset.saturating_add(criteria.limit).saturating_add(1);
                    let (ranked_ids, total) = store
                        .search_doc_ids_page(
                            space_id,
                            criteria.query.as_deref(),
                            criteria.include_deleted,
                            None,
                            fetch_limit,
                        )
                        .await?;

                    let has_more = ranked_ids.len() > offset.saturating_add(criteria.limit);
                    let ids: Vec<_> = ranked_ids
                        .into_iter()
                        .skip(offset)
                        .take(criteria.limit)
                        .collect();
                    let next_cursor = has_more.then(|| (offset + criteria.limit).to_string());

                    let docs = store
                        .get_many(space_id, &ids, criteria.include_deleted)
                        .await?;
                    return Ok(PaginatedResult::new(docs, next_cursor, has_more, total));
                }

                let cursor_id = criteria
                    .cursor
                    .as_ref()
                    .map(|cursor| {
                        crate::parse_id(cursor, TableName::Doc)
                            .map_err(|e| StorageError::QueryError(e.to_string()))
                    })
                    .transpose()?;

                if criteria.limit == 0 {
                    let (_, total) = store
                        .search_doc_ids_page(
                            space_id,
                            criteria.query.as_deref(),
                            criteria.include_deleted,
                            cursor_id.as_ref(),
                            0,
                        )
                        .await?;
                    return Ok(PaginatedResult::new(Vec::new(), None, false, total));
                }

                let (mut ids, total) = store
                    .search_doc_ids_page(
                        space_id,
                        criteria.query.as_deref(),
                        criteria.include_deleted,
                        cursor_id.as_ref(),
                        criteria.limit.saturating_add(1),
                    )
                    .await?;

                let has_more = ids.len() > criteria.limit;

                let next_cursor = if ids.len() > criteria.limit {
                    let _ = ids.pop();
                    ids.last().map(|id| {
                        let id_str = id.to_string();
                        id_str
                            .split(':')
                            .nth(1)
                            .map_or_else(|| id_str.clone(), str::to_string)
                    })
                } else {
                    None
                };

                let docs = store
                    .get_many(space_id, &ids, criteria.include_deleted)
                    .await?;
                return Ok(PaginatedResult::new(docs, next_cursor, has_more, total));
            }

            return store
                .search(criteria, |doc: &Doc, criteria| {
                    criteria.query.as_ref().is_none_or(|query| {
                        let query = query.to_lowercase();
                        doc.title.to_lowercase().contains(&query)
                            || doc.text.to_lowercase().contains(&query)
                    })
                })
                .await;
        }

        #[derive(serde::Deserialize)]
        struct CountResult {
            total: usize,
        }

        let mut conditions = Vec::new();
        let mut bindings = std::collections::BTreeMap::new();

        if !criteria.include_deleted {
            conditions.push("deleted_at IS NONE".to_string());
        }

        if let Some(ref space_id) = criteria.space_id {
            conditions.push("space_id = $space_id".to_string());
            bindings.insert(
                "space_id".to_string(),
                surrealdb::sql::Value::from(space_id.clone()),
            );
        }

        let where_prefix = if conditions.is_empty() {
            String::new()
        } else {
            format!("{} AND ", conditions.join(" AND "))
        };

        if let Some(ref query) = criteria.query {
            let tokens = lexical_query_tokens(query);
            if tokens.is_empty() {
                return Ok(PaginatedResult::new(Vec::new(), None, false, 0));
            }

            for (index, token) in tokens.iter().enumerate() {
                bindings.insert(
                    format!("query_{index}"),
                    surrealdb::sql::Value::from(token.as_str()),
                );
            }

            let mut predicate_parts = Vec::with_capacity(tokens.len());
            let mut score_parts = Vec::with_capacity(tokens.len() * 2);
            for (index, _token) in tokens.iter().enumerate() {
                let title_slot = index * 2;
                let text_slot = title_slot + 1;
                let binding = format!("$query_{index}");
                predicate_parts.push(format!(
                    "(title @{title_slot}@ {binding} OR text @{text_slot}@ {binding})"
                ));
                score_parts.push(format!("search::score({title_slot})"));
                score_parts.push(format!("search::score({text_slot})"));
            }

            let search_predicate = predicate_parts.join(" OR ");
            let score_expression = score_parts.join(" + ");
            // Parenthesised: AND binds tighter than OR, so without them every
            // term after the first escaped the space and deleted filters.
            let where_clause = format!("WHERE {where_prefix}({search_predicate})");
            let offset = ranked_search_offset(criteria.cursor.as_deref())?;

            let query = format!(
                "SELECT {DOC_PROJECTION}, ({score_expression}) AS lexical_score \
                 FROM doc {where_clause} \
                 ORDER BY lexical_score DESC, id ASC START {offset} LIMIT {}",
                criteria.limit + 1
            );

            let indexed_search = async {
                let mut response = self.client.query_bind(&query, bindings.clone()).await?;
                let docs_db: Vec<DocDb> = response
                    .take(0)
                    .map_err(|e| StorageError::QueryError(format!("Failed to parse docs: {e}")))?;

                let mut docs: Vec<Doc> = docs_db
                    .into_iter()
                    .map(Doc::try_from)
                    .collect::<Result<Vec<_>, _>>()?;

                let has_more = docs.len() > criteria.limit;
                if has_more {
                    docs.pop();
                }

                let next_cursor = has_more.then(|| (offset + criteria.limit).to_string());

                let count_query =
                    format!("SELECT count() as total FROM doc {where_clause} GROUP ALL");
                let mut count_response = self
                    .client
                    .query_bind(&count_query, bindings.clone())
                    .await?;
                let counts: Vec<CountResult> =
                    crate::response::take_first(&mut count_response, "Failed to parse doc count")?;
                let total = counts.first().map_or(0, |c| c.total);

                Ok(PaginatedResult::new(docs, next_cursor, has_more, total))
            }
            .await;

            match indexed_search {
                Ok(result) => return Ok(result),
                Err(StorageError::QueryError(message)) if missing_doc_search_index(&message) => {
                    return self
                        .fallback_lexical_search(criteria, &conditions, bindings, &tokens)
                        .await;
                }
                Err(error) => return Err(error),
            }
        }

        if let Some(cursor) = criteria.cursor.as_ref() {
            let rid = crate::parse_id(cursor, TableName::Doc)
                .map_err(|e| StorageError::QueryError(e.to_string()))?;
            conditions.push("id > $cursor".to_string());
            bindings.insert(
                "cursor".to_string(),
                surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(&rid)),
            );
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let query = format!(
            "SELECT {DOC_PROJECTION} FROM doc {where_clause} ORDER BY id LIMIT {}",
            criteria.limit + 1
        );

        let mut response = self.client.query_bind(&query, bindings.clone()).await?;

        let docs_db: Vec<DocDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse docs: {e}")))?;

        let mut docs: Vec<Doc> = docs_db
            .into_iter()
            .map(Doc::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        let has_more = docs.len() > criteria.limit;
        if has_more {
            docs.pop();
        }

        let next_cursor = if has_more {
            docs.last().map(|d| {
                d.base
                    .id
                    .to_string()
                    .split(':')
                    .nth(1)
                    .unwrap_or(&d.base.id.to_string())
                    .to_string()
            })
        } else {
            None
        };

        let count_query = format!("SELECT count() as total FROM doc {where_clause} GROUP ALL");
        let mut count_response = self.client.query_bind(&count_query, bindings).await?;
        let counts: Vec<CountResult> =
            crate::response::take_first(&mut count_response, "Failed to parse doc count")?;
        let total = counts.first().map_or(0, |c| c.total);

        Ok(PaginatedResult::new(docs, next_cursor, has_more, total))
    }
}

impl DocRepository {
    async fn fallback_lexical_search(
        &self,
        criteria: &SearchCriteria,
        conditions: &[String],
        bindings: std::collections::BTreeMap<String, surrealdb::sql::Value>,
        tokens: &[String],
    ) -> StorageResult<PaginatedResult<Doc>> {
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let query = format!("SELECT {DOC_PROJECTION} FROM doc {where_clause} ORDER BY id");
        let mut response = self.client.query_bind(&query, bindings).await?;
        let docs_db: Vec<DocDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse docs: {e}")))?;

        let mut scored_docs: Vec<(i32, Doc)> = docs_db
            .into_iter()
            .map(Doc::try_from)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter_map(|doc| {
                let title = doc.title.to_lowercase();
                let text = doc.text.to_lowercase();
                let mut score = 0_i32;

                for token in tokens {
                    if title.contains(token) {
                        score += 2;
                    }
                    if text.contains(token) {
                        score += 1;
                    }
                }

                (score > 0).then_some((score, doc))
            })
            .collect();

        scored_docs.sort_by(|(score_a, doc_a), (score_b, doc_b)| {
            score_b
                .cmp(score_a)
                .then_with(|| doc_a.base.id.to_string().cmp(&doc_b.base.id.to_string()))
        });

        let total = scored_docs.len();
        let offset = ranked_search_offset(criteria.cursor.as_deref())?;
        let mut docs: Vec<Doc> = scored_docs
            .into_iter()
            .map(|(_, doc)| doc)
            .skip(offset)
            .take(criteria.limit + 1)
            .collect();
        let has_more = docs.len() > criteria.limit;
        if has_more {
            docs.truncate(criteria.limit);
        }

        let next_cursor = has_more.then(|| (offset + criteria.limit).to_string());

        Ok(PaginatedResult::new(docs, next_cursor, has_more, total))
    }
}

fn lexical_query_tokens(query: &str) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut tokens = Vec::new();
    for token in tokenize(query) {
        if seen.insert(token.clone()) {
            tokens.push(token);
        }
    }
    tokens
}

fn missing_doc_search_index(message: &str) -> bool {
    message.contains("There was no suitable index supporting the expression")
}

fn ranked_search_offset(cursor: Option<&str>) -> StorageResult<usize> {
    cursor.map_or(Ok(0), |value| {
        value.parse::<usize>().map_err(|error| {
            StorageError::QueryError(format!("Invalid ranked search cursor `{value}`: {error}"))
        })
    })
}

#[cfg(test)]
#[path = "doc_presence_tests.rs"]
mod physical_presence_tests;
