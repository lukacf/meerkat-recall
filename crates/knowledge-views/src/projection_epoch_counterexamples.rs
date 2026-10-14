// Insert this module inside crates/knowledge-views/src/lib.rs::tests.
// It reuses the existing register_space and entity fixtures.
mod projection_epoch_counterexamples {
    use super::*;
    use elephant_storage::canonical_commit::CanonicalRepository;

    async fn storage(embedded: bool) -> StorageClient {
        let storage = if embedded {
            StorageClient::new_embedded()
                .await
                .expect("embedded storage")
        } else {
            let storage = StorageClient::new(elephant_storage::StorageConfig {
                url: "mem://".to_string(),
                database: format!("projection_epoch_{}", Uuid::new_v4()),
                ..elephant_storage::StorageConfig::default()
            })
            .await
            .expect("Surreal memory storage");
            elephant_migrations::apply_schema(&storage)
                .await
                .expect("explicit test schema");
            storage
        };
        register_space(&storage).await;
        storage
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain canonical fixture ownership through both missing-epoch and no-initialization assertions."
    )]
    async fn missing_epoch_remains_absent(embedded: bool) {
        let storage = storage(embedded).await;
        let canonical = CanonicalRepository::new(storage.clone());
        let missing = "unconfigured-read-scope";
        assert_eq!(
            canonical.observe_epoch(missing).await.expect("absence"),
            None
        );
        assert!(verify_projection_epoch(&storage, missing, "")
            .await
            .is_err());
        assert_eq!(
            canonical
                .observe_epoch(missing)
                .await
                .expect("still absent"),
            None
        );
        assert!(
            verify_projection_epoch(&storage, missing, "old-observed-epoch")
                .await
                .is_err()
        );
        assert_eq!(
            canonical
                .observe_epoch(missing)
                .await
                .expect("final absence"),
            None,
            "revalidation must not initialize missing canonical state"
        );
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Preserve original storage cleanup after physical-row and foreign-epoch refusal assertions."
    )]
    async fn foreign_epoch_is_rejected_without_repair(embedded: bool) {
        let storage = storage(embedded).await;
        if let Some(store) = storage.embedded_store() {
            store
                .mutate(|tx| {
                    tx.upsert_extension_row(
                        "canonical_epoch",
                        serde_json::json!({
                            "id": "canonical_epoch:space",
                            "space_id": "foreign-owner",
                            "epoch": "foreign-observed-token"
                        }),
                    )?;
                    Ok(())
                })
                .await
                .expect("inject mismatched physical epoch owner");
        } else {
            storage
                .query_checked(
                    "UPDATE canonical_epoch:space SET space_id = 'foreign-owner', epoch = 'foreign-observed-token';",
                )
                .await
                .expect("inject mismatched physical epoch owner");
        }
        assert!(
            verify_projection_epoch(&storage, "space", "foreign-observed-token")
                .await
                .is_err(),
            "equal token text does not authorize a foreign epoch row"
        );
        // Check the original physical row, not only the verifier's error:
        // replacing the corrupt row with a fresh owned epoch is also a write.
        let row = if let Some(store) = storage.embedded_store() {
            let rows = store
                .extension_rows_page("canonical_epoch", "foreign-owner", None, 2)
                .await
                .expect("physical row page");
            assert_eq!(rows.len(), 1, "the original foreign epoch remains present");
            let (key, row) = rows.into_iter().next().expect("epoch retained");
            assert_eq!(key, "canonical_epoch:space");
            row
        } else {
            let mut response = storage
                .query_checked("SELECT space_id, epoch FROM canonical_epoch:space;")
                .await
                .expect("physical row");
            let rows: Vec<serde_json::Value> = response.take(0).expect("epoch projection");
            assert_eq!(rows.len(), 1);
            rows.into_iter().next().expect("epoch retained")
        };
        assert_eq!(row["space_id"], "foreign-owner");
        assert_eq!(row["epoch"], "foreign-observed-token");
        assert!(
            CanonicalRepository::new(storage)
                .observe_epoch("space")
                .await
                .is_err(),
            "the foreign original must remain invalid, not be repaired by a read"
        );
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the canonical handle through the real-writer invalidation and original epoch assertions."
    )]
    async fn real_writer_invalidates_the_original_epoch(embedded: bool) {
        let storage = storage(embedded).await;
        let canonical = CanonicalRepository::new(storage.clone());
        let original = canonical
            .current_epoch("space")
            .await
            .expect("original epoch");
        verify_projection_epoch(&storage, "space", &original)
            .await
            .expect("unchanged original passes");
        assert_eq!(
            canonical.current_epoch("space").await.expect("unchanged"),
            original
        );
        EntityRepository::new(storage.clone())
            .create(entity("actual canonical writer"))
            .await
            .expect("real canonical mutation");
        let changed = canonical
            .current_epoch("space")
            .await
            .expect("changed epoch");
        assert_ne!(changed, original);
        assert!(
            verify_projection_epoch(&storage, "space", &original)
                .await
                .is_err(),
            "a read-only verifier must retain the original fence"
        );
        verify_projection_epoch(&storage, "space", &changed)
            .await
            .expect("new explicitly captured original passes");
        assert_eq!(
            canonical.current_epoch("space").await.expect("no rewrite"),
            changed
        );
    }

    #[tokio::test]
    async fn embedded_missing_epoch_read_never_initializes_state() {
        missing_epoch_remains_absent(true).await;
    }

    #[tokio::test]
    async fn surreal_missing_epoch_read_never_initializes_state() {
        missing_epoch_remains_absent(false).await;
    }

    #[tokio::test]
    async fn embedded_foreign_epoch_read_never_repairs_state() {
        foreign_epoch_is_rejected_without_repair(true).await;
    }

    #[tokio::test]
    async fn surreal_foreign_epoch_read_never_repairs_state() {
        foreign_epoch_is_rejected_without_repair(false).await;
    }

    #[tokio::test]
    async fn embedded_epoch_read_preserves_real_writer_fence() {
        real_writer_invalidates_the_original_epoch(true).await;
    }

    #[tokio::test]
    async fn surreal_epoch_read_preserves_real_writer_fence() {
        real_writer_invalidates_the_original_epoch(false).await;
    }
}
