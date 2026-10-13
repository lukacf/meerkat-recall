//! Managed media asset repository implementation for `SurrealDB`.
//!
//! Read contract: `get`/`list` and the create/update select-backs return the
//! metadata view with `payload_base64 = None` — prod assets carry hundreds of
//! KB of base64 each and metadata readers were deserializing all of it
//! (issue #34). [`MediaAssetRepository::get_with_payload`] is the only
//! payload-bearing read. Note `update` still writes the full record CONTENT:
//! round-tripping a metadata read through `update` clears the stored payload,
//! so fetch via `get_with_payload` when mutating payload-bearing assets.

#![allow(clippy::items_after_statements)]

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;

use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::Entity;
use elephant_types::records::MediaAsset;
use elephant_types::{Link, RecordId};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;
use crate::require::required_field;

/// Metadata projection: every field except `payload_base64`. The payload is
/// selected only by `get_with_payload`, which appends it to this projection.
const MEDIA_ASSET_METADATA_PROJECTION: &str = r"
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
    kind,
    media_type,
    storage_path,
    size_bytes,
    sha256,
    duration_ms,
    label
";

#[derive(Deserialize)]
struct MediaAssetDb {
    id: surrealdb::sql::Thing,
    space_id: Option<String>,
    created_at: Option<chrono::DateTime<Utc>>,
    updated_at: Option<chrono::DateTime<Utc>>,
    created_by: Option<String>,
    deleted_at: Option<chrono::DateTime<Utc>>,
    provenance: Option<Provenance>,
    security: Option<Security>,
    subjects: Option<Vec<Link<Entity>>>,
    subjects_state: Option<SubjectsState>,
    kind: Option<elephant_types::records::DocMediaKind>,
    media_type: Option<String>,
    storage_path: Option<String>,
    size_bytes: Option<u64>,
    sha256: Option<String>,
    /// Absent from the metadata projection; explicit default keeps the same
    /// proxy deserializing under both projections.
    #[serde(default)]
    payload_base64: Option<String>,
    duration_ms: Option<u64>,
    label: Option<String>,
}

impl TryFrom<MediaAssetDb> for MediaAsset {
    type Error = StorageError;

    fn try_from(db: MediaAssetDb) -> Result<Self, Self::Error> {
        Ok(Self {
            base: BaseFields {
                id: crate::surreal::record_id_from_thing(&db.id)
                    .map_err(|e| StorageError::QueryError(e.to_string()))?,
                space_id: required_field(db.space_id, "space_id")?,
                created_at: required_field(db.created_at, "created_at")?,
                updated_at: required_field(db.updated_at, "updated_at")?,
                created_by: required_field(db.created_by, "created_by")?,
                deleted_at: db.deleted_at,
                provenance: required_field(db.provenance, "provenance")?,
                security: required_field(db.security, "security")?,
                subjects: required_field(db.subjects, "subjects")?,
                subjects_state: required_field(db.subjects_state, "subjects_state")?,
            },
            kind: required_field(db.kind, "kind")?,
            media_type: required_field(db.media_type, "media_type")?,
            storage_path: required_field(db.storage_path, "storage_path")?,
            size_bytes: required_field(db.size_bytes, "size_bytes")?,
            sha256: required_field(db.sha256, "sha256")?,
            payload_base64: db.payload_base64,
            duration_ms: db.duration_ms,
            label: db.label,
        })
    }
}

/// Managed media asset repository.
#[derive(Clone, Debug)]
pub struct MediaAssetRepository {
    client: StorageClient,
}

impl MediaAssetRepository {
    /// Creates a new managed media asset repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Drops the inline payload so metadata reads return the same view on
    /// both backends (the embedded store keeps full records).
    fn without_payload(mut asset: MediaAsset) -> MediaAsset {
        asset.payload_base64 = None;
        asset
    }

    /// Gets an asset including its inline `payload_base64`.
    ///
    /// This is the only payload-bearing read; `get`/`list` and the
    /// create/update select-backs all serve the metadata view. Use it solely
    /// where the bytes are needed (the pipeline's media loader).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or result parsing fails.
    pub async fn get_with_payload(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<MediaAsset>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<MediaAsset>(space_id, id, false).await;
        }

        // Record access (`FROM ONLY $id`) instead of `FROM media_asset WHERE
        // id = $id`: SurrealDB's planner has no id-equality fast path, so the
        // table-scan form deserialized every asset payload in the table on
        // each point lookup (issue #34). The space_id/deleted_at WHERE clause
        // preserves space isolation: a cross-space id resolves to NONE.
        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {MEDIA_ASSET_METADATA_PROJECTION}, payload_base64 FROM ONLY $id WHERE space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id
                },
            )
            .await?;

        let item: Option<MediaAssetDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse media_asset: {e}")))?;

        item.map(MediaAsset::try_from).transpose()
    }

    pub(crate) fn media_asset_to_value(asset: &MediaAsset) -> StorageResult<surrealdb::sql::Value> {
        let mut obj = crate::surreal::base_fields_to_object(&asset.base)?;
        obj.insert(
            "kind".to_string(),
            crate::surreal::enum_to_surreal(&asset.kind)?,
        );
        obj.insert("media_type".to_string(), asset.media_type.clone().into());
        obj.insert(
            "storage_path".to_string(),
            asset.storage_path.clone().into(),
        );
        obj.insert(
            "size_bytes".to_string(),
            surrealdb::sql::Value::from(asset.size_bytes),
        );
        obj.insert("sha256".to_string(), asset.sha256.clone().into());
        obj.insert(
            "payload_base64".to_string(),
            asset
                .payload_base64
                .as_ref()
                .map_or(surrealdb::sql::Value::None, |payload| {
                    surrealdb::sql::Value::from(payload.clone())
                }),
        );
        obj.insert(
            "duration_ms".to_string(),
            asset
                .duration_ms
                .map_or(surrealdb::sql::Value::None, surrealdb::sql::Value::from),
        );
        obj.insert(
            "label".to_string(),
            asset
                .label
                .as_ref()
                .map_or(surrealdb::sql::Value::None, |label| {
                    surrealdb::sql::Value::from(label.clone())
                }),
        );
        Ok(surrealdb::sql::Value::Object(obj))
    }
}

#[async_trait]
impl Repository<MediaAsset> for MediaAssetRepository {
    async fn create(&self, record: MediaAsset) -> StorageResult<MediaAsset> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| Ok(mutation.create(record.clone())?))
                .await
                .map(Self::without_payload);
        }

        let surreal_record = Self::media_asset_to_value(&record)?;

        let create_response = self
            .client
            .query_bind(
                "CREATE $id CONTENT $record;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => surreal_record,
                },
            )
            .await
            .map_err(|e| {
                StorageError::classify_query_error(format!("Failed to create media_asset: {e}"))
            })?;
        create_response.check().map_err(|e| {
            StorageError::classify_query_error(format!("Failed to create media_asset: {e}"))
        })?;

        // Metadata-only select-back via record access: the caller already
        // holds the payload it just wrote.
        let mut response = self
            .client
            .query_bind(
                &format!("SELECT {MEDIA_ASSET_METADATA_PROJECTION} FROM ONLY $id"),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(&record.base.id) },
            )
            .await?;

        let item: Option<MediaAssetDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created media_asset: {e}"))
        })?;

        item.map_or_else(|| Ok(Self::without_payload(record)), MediaAsset::try_from)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<MediaAsset>> {
        if let Some(store) = self.client.embedded_store() {
            return Ok(store
                .get::<MediaAsset>(space_id, id, false)
                .await?
                .map(Self::without_payload));
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {MEDIA_ASSET_METADATA_PROJECTION} FROM ONLY $id WHERE space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id
                },
            )
            .await?;

        let item: Option<MediaAssetDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse media_asset: {e}")))?;

        item.map(MediaAsset::try_from).transpose()
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: MediaAsset,
    ) -> StorageResult<MediaAsset> {
        if let Some(store) = self.client.embedded_store() {
            let target_space_id = space_id.to_string();
            let target_id = id.clone();
            record.base.space_id.clone_from(&target_space_id);
            record.base.id.clone_from(&target_id);

            return store
                .mutate(|mutation| {
                    let Some(_existing) =
                        mutation.get::<MediaAsset>(&target_space_id, &target_id, false)?
                    else {
                        return Err(StorageError::NotFound(target_id.to_string()));
                    };
                    Ok(mutation.replace(record.clone())?)
                })
                .await
                .map(Self::without_payload);
        }

        record.base.space_id = space_id.to_string();
        record.base.id = id.clone();
        let surreal_record = Self::media_asset_to_value(&record)?;
        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id CONTENT $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {MEDIA_ASSET_METADATA_PROJECTION}"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => surreal_record,
                },
            )
            .await?;

        let items: Vec<MediaAssetDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated media_asset: {e}"))
        })?;

        let item = items
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(id.to_string()))?;

        MediaAsset::try_from(item)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<MediaAsset>(space_id, id).await;
        }

        self.client
            .query_bind(
                "UPDATE $id SET deleted_at = time::now() WHERE space_id = $space_id;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id
                },
            )
            .await?
            .check()
            .map_err(|e| {
                StorageError::classify_query_error(format!("Failed to delete media_asset: {e}"))
            })?;
        Ok(())
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<MediaAsset>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items = store
                .list::<MediaAsset>(space_id, limit.saturating_add(1), cursor, false)
                .await?;
            if items.len() > limit {
                items.truncate(limit);
            }
            return Ok(items.into_iter().map(Self::without_payload).collect());
        }

        let mut response = if let Some(cursor) = cursor {
            self.client
                .query_bind(
                    &format!(
                        "SELECT {MEDIA_ASSET_METADATA_PROJECTION} FROM media_asset WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT $limit"
                    ),
                    crate::bindings! {
                        "space_id" => space_id,
                        "cursor" => crate::surreal::thing_from_record_id(cursor),
                        "limit" => limit
                    },
                )
                .await?
        } else {
            self.client
                .query_bind(
                    &format!(
                        "SELECT {MEDIA_ASSET_METADATA_PROJECTION} FROM media_asset WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT $limit"
                    ),
                    crate::bindings! {
                        "space_id" => space_id,
                        "limit" => limit
                    },
                )
                .await?
        };

        let items: Vec<MediaAssetDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse media_asset list: {e}"))
        })?;

        items.into_iter().map(MediaAsset::try_from).collect()
    }
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

    fn test_asset(space_id: &str) -> MediaAsset {
        MediaAsset {
            base: BaseFields::new(
                RecordId::new_v4(TableName::MediaAsset),
                space_id.to_string(),
                "tester".to_string(),
            ),
            kind: elephant_types::records::DocMediaKind::Image,
            media_type: "image/jpeg".to_string(),
            storage_path: "media_asset/test".to_string(),
            size_bytes: 4,
            sha256: "abcd".to_string(),
            payload_base64: Some("AQIDBA==".to_string()),
            duration_ms: None,
            label: Some("frame.jpg".to_string()),
        }
    }

    async fn assert_roundtrip_and_payload_split(repo: &MediaAssetRepository) {
        let asset = test_asset("default");

        let created = repo.create(asset.clone()).await.expect("create asset");
        assert_eq!(
            created.payload_base64, None,
            "create returns the metadata view"
        );

        // get() serves the metadata view: identical record minus the payload.
        let loaded = repo
            .get("default", &asset.base.id)
            .await
            .expect("get asset")
            .expect("stored asset");
        let mut expected_metadata = asset.clone();
        expected_metadata.payload_base64 = None;
        assert_eq!(loaded, expected_metadata);

        let listed = repo.list("default", 10, None).await.expect("list assets");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].payload_base64, None, "list omits payloads");

        // get_with_payload() is the one payload-bearing read.
        let with_payload = repo
            .get_with_payload("default", &asset.base.id)
            .await
            .expect("get asset with payload")
            .expect("stored asset");
        assert_eq!(with_payload, asset);
    }

    async fn assert_cross_space_get_is_none(repo: &MediaAssetRepository) {
        let asset = test_asset("space_a");
        repo.create(asset.clone()).await.expect("create asset");

        // Space isolation: a known id must not resolve from another space,
        // through either the metadata or the payload read.
        let cross_space = repo.get("space_b", &asset.base.id).await.expect("get");
        assert!(cross_space.is_none(), "cross-space get must return None");
        let cross_space_payload = repo
            .get_with_payload("space_b", &asset.base.id)
            .await
            .expect("get with payload");
        assert!(
            cross_space_payload.is_none(),
            "cross-space get_with_payload must return None"
        );

        // The owning space still sees it.
        assert!(repo
            .get("space_a", &asset.base.id)
            .await
            .expect("get")
            .is_some());
    }

    async fn assert_deleted_get_is_none(repo: &MediaAssetRepository) {
        let asset = test_asset("default");
        repo.create(asset.clone()).await.expect("create asset");
        repo.delete("default", &asset.base.id)
            .await
            .expect("delete asset");

        assert!(repo
            .get("default", &asset.base.id)
            .await
            .expect("get")
            .is_none());
        assert!(repo
            .get_with_payload("default", &asset.base.id)
            .await
            .expect("get with payload")
            .is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client in the original fixture scope through assertions and cleanup."
    )]
    async fn test_media_asset_repository_roundtrip_in_memory() {
        let client = StorageClient::new_memory().await.expect("memory client");
        let repo = MediaAssetRepository::new(client);
        assert_roundtrip_and_payload_split(&repo).await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client in the original fixture scope through assertions and cleanup."
    )]
    async fn test_media_asset_repository_roundtrip_embedded() {
        let client = StorageClient::new_embedded()
            .await
            .expect("embedded client");
        let repo = MediaAssetRepository::new(client);
        assert_roundtrip_and_payload_split(&repo).await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client in the original fixture scope through assertions and cleanup."
    )]
    async fn test_media_asset_cross_space_get_is_none_in_memory() {
        let client = StorageClient::new_memory().await.expect("memory client");
        let repo = MediaAssetRepository::new(client);
        assert_cross_space_get_is_none(&repo).await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client in the original fixture scope through assertions and cleanup."
    )]
    async fn test_media_asset_cross_space_get_is_none_embedded() {
        let client = StorageClient::new_embedded()
            .await
            .expect("embedded client");
        let repo = MediaAssetRepository::new(client);
        assert_cross_space_get_is_none(&repo).await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client in the original fixture scope through assertions and cleanup."
    )]
    async fn test_media_asset_deleted_get_is_none_in_memory() {
        let client = StorageClient::new_memory().await.expect("memory client");
        let repo = MediaAssetRepository::new(client);
        assert_deleted_get_is_none(&repo).await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client in the original fixture scope through assertions and cleanup."
    )]
    async fn test_media_asset_deleted_get_is_none_embedded() {
        let client = StorageClient::new_embedded()
            .await
            .expect("embedded client");
        let repo = MediaAssetRepository::new(client);
        assert_deleted_get_is_none(&repo).await;
    }
}
