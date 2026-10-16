//! Real persistent source erasure through Recall, SDK administration and RocksDB.
use super::*;
use std::path::Path;

const CERTIFICATE: &str = ".database.recall-physical-checkpoint.json";
const PRIVATE_PAYLOAD: &str = "PERSISTENT_PHYSICAL_ERASURE_CANARY_903421";

async fn open(root: &Path) -> StorageClient {
    StorageClient::new_with_durability(
        StorageConfig {
            url: format!("rocksdb://{}", root.join("database").display()),
            ..StorageConfig::default()
        },
        None,
        Some(root.join("independent-journal")),
    )
    .await
    .expect("persistent native store")
}

async fn initialize(storage: &StorageClient) {
    storage
        .query_checked(concat!(
            include_str!("../../migrations/sql/031_source_publication.surql"),
            "\n",
            include_str!("../../migrations/sql/046_source_support_index_fence.surql")
        ))
        .await
        .unwrap();
    crate::reference_fence::install_native_reference_guards(storage, &[])
        .await
        .expect("private reference schema");
    let config: SpaceConfig = serde_json::from_value(serde_json::json!({
        "id": RecordId::new_v4(TableName::SpaceConfig), "space_id": "erase-a",
        "created_at": Utc::now(), "updated_at": Utc::now(), "created_by": "test",
        "security": {"policy_id":"default", "level":0, "labels":[], "handling":[]},
        "provenance": {}, "subjects": [], "subjects_state":"complete",
        "embedding_provider":"fake", "embedding_model":"test", "embedding_dims":8
    }))
    .unwrap();
    SpaceConfigRepository::new(storage.clone())
        .create(config)
        .await
        .unwrap();
}

fn certificate(root: &Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(root.join(CERTIFICATE)).unwrap()).unwrap()
}

fn source_input(external: &str, payload: &str) -> AcceptSourceRevision {
    let mut input = request("erase-a", external);
    input.source.metadata = serde_json::json!({"_manual_doc":{"text":payload.repeat(256)}});
    input
}

#[test]
#[expect(
    clippy::case_sensitive_file_extension_comparisons,
    reason = "The fixture checks the exact lowercase .sst suffix emitted by the native engine."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
fn rocksdb_source_erasure_retires_generations_preserves_independent_source_and_reopens() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let writer = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("original native writer runtime");
    let (private_id, independent_id, journal_identity) = writer.block_on(async {
        let storage = open(root).await;
        initialize(&storage).await;
        let publication = SourcePublicationRepository::new(storage.clone());
        let private = publication
            .accept(source_input("physical-private", PRIVATE_PAYLOAD))
            .await
            .unwrap();
        let independent = publication
            .accept(source_input(
                "physical-independent",
                "independent live source",
            ))
            .await
            .unwrap();
        let private_id = private.source_id.clone();
        let independent_id = independent.source_id.clone();
        let journal_identity = storage.erasure_journal().unwrap().identity().to_owned();
        // A real administrative checkpoint first forces accepted private values
        // into persistent SST/blob generations before the erasure transaction.
        storage.persist_restored_state().await.unwrap();
        let original = certificate(root);
        let original_sequence = original["current"]["sequence"].as_u64().unwrap();
        let original_generations = original["current"]["current_payload_generations"]
            .as_array()
            .unwrap();
        assert!(original_generations
            .iter()
            .any(|row| row["name"].as_str().unwrap().ends_with(".sst")));
        publication
            .erase_live(mutation(&publication, &private, "physical-erase").await)
            .await
            .unwrap();
        assert!(storage.recovery_status().ready);
        let final_receipt = certificate(root);
        assert_eq!(
            final_receipt["current"]["sequence"].as_u64(),
            Some(original_sequence + 2)
        );
        assert_eq!(
            final_receipt["previous"]["sequence"].as_u64(),
            Some(original_sequence + 1)
        );
        assert_eq!(
            final_receipt["previous"]["old_engine_identity"],
            original["current"]["engine_identity"]
        );
        assert_eq!(
            final_receipt["previous"]["engine_identity"],
            final_receipt["current"]["old_engine_identity"]
        );
        assert_ne!(
            final_receipt["current"]["engine_identity"],
            original["current"]["engine_identity"]
        );
        assert_eq!(
            std::fs::read_to_string(root.join("database/IDENTITY"))
                .unwrap()
                .trim(),
            final_receipt["current"]["engine_identity"]
                .as_str()
                .unwrap()
        );
        assert!(final_receipt["previous"]["original_payload_generations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["name"].as_str().unwrap().starts_with("MANIFEST-")));
        let sources = SourceRepository::new(storage.clone());
        assert!(sources.get("erase-a", &private_id).await.unwrap().is_none());
        assert!(sources
            .get("erase-a", &independent_id)
            .await
            .unwrap()
            .is_some());
        let intents = storage.erasure_journal().unwrap().intents().unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].source_id, private_id);
        assert!(!serde_json::to_string(&intents)
            .unwrap()
            .contains(PRIVATE_PAYLOAD));
        (private_id, independent_id, journal_identity)
    });
    // The local SDK router retains the native engine after its last client is
    // dropped. Closing its runtime releases that owner before the one reopen;
    // no timer or failed-open retry stands in for the original engine closing.
    drop(writer);
    // Reopen the actual on-disk engine and independently retained original journal.
    let reader = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("reopened native reader runtime");
    reader.block_on(async {
        let reopened = open(root).await;
        assert!(reopened.recovery_status().ready);
        assert_eq!(
            reopened.erasure_journal().unwrap().identity(),
            journal_identity
        );
        let sources = SourceRepository::new(reopened);
        assert!(sources.get("erase-a", &private_id).await.unwrap().is_none());
        assert!(sources
            .get("erase-a", &independent_id)
            .await
            .unwrap()
            .is_some());
    });
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn rocksdb_unclassified_historical_file_leaves_erase_pending_and_same_process_recovery_closed(
) {
    let directory = tempfile::tempdir().unwrap();
    let storage = open(directory.path()).await;
    initialize(&storage).await;
    let publication = SourcePublicationRepository::new(storage.clone());
    let accepted = publication
        .accept(source_input("physical-refusal", PRIVATE_PAYLOAD))
        .await
        .unwrap();
    let original_journal = storage.erasure_journal().unwrap().identity().to_owned();
    std::fs::write(
        directory
            .path()
            .join("database/unclassified-history.backup"),
        PRIVATE_PAYLOAD,
    )
    .unwrap();
    let result = publication
        .erase_live(mutation(&publication, &accepted, "physical-refusal-erase").await)
        .await;
    assert!(result.is_err());
    assert!(!storage.recovery_status().ready);
    assert!(storage
        .begin_maintenance(
            crate::recovery_barrier::RecoveryPhase::Replaying,
            std::time::Duration::from_millis(5)
        )
        .await
        .is_err());
    assert_eq!(
        storage.erasure_journal().unwrap().identity(),
        original_journal
    );
    assert_eq!(
        storage.erasure_journal().unwrap().intents().unwrap().len(),
        1
    );
    assert!(!directory.path().join(CERTIFICATE).exists());
    assert!(directory
        .path()
        .join("database/unclassified-history.backup")
        .exists());
}
