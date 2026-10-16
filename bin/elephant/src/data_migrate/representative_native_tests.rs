//! Hash-bound Python originals through actual native transfer adapters.
//! These are representation/schema controls, not graph closure or adoption proof.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::source_spool::{typed, TypedRow};
use super::*;
use elephant_storage::transfer_batch::{
    import_batch, import_embedding_batch, strict_json, TransferRecord,
};
use elephant_storage::{StorageError, StorageResult};
use serde::Deserialize;
use std::collections::BTreeSet;

const MANIFEST_SHA: &str = "3fb24272242b0340af031207e14f684224919b2d92fa3a98bdfd1b87fce06c72";
const BUNDLE: &str =
    include_str!("../../../../scripts/migration/fixtures/representative-native-v1.json");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Bundle {
    manifest_raw: String,
    rows: BTreeMap<String, String>,
}

struct Selection {
    ordinary: BTreeMap<String, Vec<TransferRecord>>,
    embeddings: Vec<NodeEmbedding>,
    receipt: serde_json::Value,
}

fn sha(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn selection() -> Selection {
    let bundle: Bundle = serde_json::from_value(strict_json(BUNDLE).unwrap()).unwrap();
    assert_eq!(sha(bundle.manifest_raw.as_bytes()), MANIFEST_SHA);
    let manifest = strict_json(&bundle.manifest_raw).unwrap();
    assert_eq!(manifest["format"], "recall-synthetic-representative-v1");
    assert_eq!(manifest["selected_rows"], 79);
    assert_eq!(manifest["row_bytes"], 144_257);
    assert_eq!(manifest["graph_reference_closure"], false);
    assert_eq!(
        manifest["source"]["helper_sha256"],
        "7bc9527af0fbf73cacf481addb363d91d3a593f596c4933e260761cb2eaa7fe5"
    );
    assert_eq!(
        manifest["source"]["driver_sha256"],
        "e2b18880afd6eda6cb0d778d83f30b216cc54a97c884aa16b68509e0049e06d2"
    );
    assert_eq!(
        manifest["source"]["selector_sha256"],
        "aaa065f1899cea7bb0fcf2c33b1417ccfb6573ff165d635ec7d211ecf0e2657d"
    );
    let declarations = manifest["selection"].as_array().unwrap();
    assert_eq!(declarations.len(), bundle.rows.len());
    let mut ordinary = BTreeMap::<String, Vec<TransferRecord>>::new();
    let mut embeddings = Vec::new();
    let mut receipt = None;
    let mut identities = BTreeSet::new();
    let mut paths = BTreeSet::new();
    let mut bytes = 0;
    for declaration in declarations {
        let path = declaration["path"].as_str().unwrap();
        assert!(paths.insert(path));
        let raw = bundle.rows.get(path).unwrap();
        assert_eq!(
            u64::try_from(raw.len()).unwrap(),
            declaration["bytes"].as_u64().unwrap()
        );
        assert!(raw.len() <= 1024 * 1024);
        bytes += raw.len();
        assert_eq!(sha(raw.as_bytes()), declaration["sha256"].as_str().unwrap());
        let value = strict_json(raw).unwrap();
        assert_eq!(value["id"], declaration["id"]);
        assert!(identities.insert(value["id"].as_str().unwrap().to_owned()));
        let table = declaration["table"].as_str().unwrap();
        let record = typed(table, raw).unwrap_or_else(|error| panic!("{path}: {error}"));
        match record {
            TypedRow::Ordinary(record) => {
                assert_eq!(declaration["disposition"], "strict-typed-native-roundtrip");
                assert_eq!(record.base().space_id, "default");
                ordinary.entry(table.into()).or_default().push(record);
            }
            TypedRow::Embedding(record) => {
                assert_eq!(
                    declaration["disposition"],
                    "partitioned-vector-native-roundtrip"
                );
                assert_eq!(record.dims, 3072);
                embeddings.push(record);
            }
            TypedRow::Receipt(value) => {
                assert_eq!(declaration["disposition"], "source-global-retained-control");
                assert!(receipt.replace(value).is_none());
            }
            TypedRow::Space(_) => panic!("representative selection declares no space config"),
        }
    }
    assert_eq!(bytes, 144_257);
    assert_eq!(ordinary.values().map(Vec::len).sum::<usize>(), 76);
    assert_eq!(embeddings.len(), 2);
    let entities = ordinary["entity"]
        .iter()
        .map(TransferRecord::id)
        .collect::<BTreeSet<_>>();
    for record in &ordinary["rel"] {
        let TransferRecord::Rel(rel) = record else {
            unreachable!()
        };
        assert!(entities.contains(rel.subject.id()));
        assert!(entities.contains(rel.object.id()));
    }
    Selection {
        ordinary,
        embeddings,
        receipt: receipt.unwrap(),
    }
}

async fn native_digest(client: &StorageClient) -> StorageResult<String> {
    let mut digest = Sha256::new();
    let mut tables = client.list_tables().await?;
    tables.sort();
    tables.dedup();
    for table in tables {
        digest.update(serde_json::to_vec(&serde_json::json!(["table", table])).unwrap());
        let mut cursor = None;
        loop {
            let rows = streaming::fingerprints(client, &table, cursor.clone())
                .await
                .map_err(|error| StorageError::DataIntegrityError(error.to_string()))?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                digest.update(
                    serde_json::to_vec(&serde_json::json!([table, row.id, row.sha256])).unwrap(),
                );
            }
            let next = rows.last().unwrap().cursor.clone();
            assert_ne!(cursor.as_ref(), Some(&next));
            cursor = Some(next);
        }
    }
    Ok(hex::encode(digest.finalize()))
}

#[test]
fn hash_bound_python_originals_pass_strict_typed_paths_and_reject_field_loss() {
    let selected = selection();
    let mut unknown = selected.ordinary["audit_log"][0].json().unwrap();
    unknown["unrecognized_original_field"] = true.into();
    assert!(TransferRecord::from_json("audit_log", &unknown.to_string()).is_err());
    let mut invalid_receipt = selected.receipt;
    invalid_receipt["checksum"] = "unknown-authority".into();
    assert!(typed("migration_history", &invalid_receipt.to_string()).is_err());
    assert!(typed("migration_history", r#"{"id":"first","id":"last"}"#).is_err());
    let deleted = selected.ordinary["doc"]
        .iter()
        .filter(|row| row.base().deleted_at.is_some())
        .count();
    assert_eq!(deleted, 2);
    assert!(selected.ordinary["event"].iter().any(|row| {
        matches!(row, TransferRecord::Event(event)
        if event.metadata.is_object()
            && event.metadata["original_values"].as_array().is_some_and(|values| {
                values.len() == 2
                    && values[0]["original_ordinal"].is_number()
                    && values[1].is_null()
            }))
    }));
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original client through transfer custody, all-table native equality, rollback and final journal initialization checks."
)]
async fn all_representative_tables_preserve_native_payloads_under_full_schema_and_custody() {
    let mut selected = selection();
    let client = StorageClient::new_memory().await.unwrap();
    let digest = MANIFEST_SHA.to_owned();
    let bound = digest.clone();
    assert!(import_batch(&client, &bound, &selected.ordinary["entity"])
        .await
        .is_err());
    client
        .with_transfer_barrier(&digest, move |exclusive| async move {
            elephant_migrations::apply_schema(&exclusive)
                .await
                .map_err(StorageError::DataIntegrityError)?;
            let baseline = native_digest(&exclusive).await?;
            // Real ENFORCED schema must reject an edge before its bound endpoints.
            let missing = import_batch(&exclusive, &bound, &selected.ordinary["rel"])
                .await
                .unwrap_err();
            assert!(missing.to_string().contains("does not exist"), "{missing}");
            assert_eq!(native_digest(&exclusive).await?, baseline);
            assert!(
                import_batch(&exclusive, &"b".repeat(64), &selected.ordinary["entity"])
                    .await
                    .is_err()
            );
            assert_eq!(native_digest(&exclusive).await?, baseline);
            let manager = PartitionManager::new();
            let mut ordinary_written = 0;
            let mut vector_written = 0;
            let mut checked_tables = BTreeSet::new();
            for table in SUPPORTED_TABLES {
                if *table == "chunk_embedding" {
                    let tables =
                        import_embedding_batch(&exclusive, &bound, &manager, &selected.embeddings)
                            .await?;
                    let expected = selected
                        .embeddings
                        .iter()
                        .map(|row| embedding_partition(row).unwrap())
                        .collect::<BTreeSet<_>>();
                    assert_eq!(tables.into_iter().collect::<BTreeSet<_>>(), expected);
                    vector_written += selected.embeddings.len();
                    continue;
                }
                let Some(records) = selected.ordinary.remove(*table) else {
                    continue;
                };
                let mut expected = records
                    .iter()
                    .map(|row| elephant_storage::surreal::thing_from_record_id(row.id()))
                    .collect::<Vec<_>>();
                expected.sort();
                expected.dedup_by(|left, right| (*left).cmp(right).is_eq());
                import_batch(&exclusive, &bound, &records)
                    .await
                    .map_err(|error| {
                        StorageError::DataIntegrityError(format!("representative {table}: {error}"))
                    })?;
                // The adapter checks full native images inside the transaction and
                // exact typed readback, including timestamps/deleted/archive fields.
                let actual = streaming::fingerprints(&exclusive, table, None)
                    .await
                    .map_err(|error| StorageError::DataIntegrityError(error.to_string()))?;
                let mut actual_ids = actual
                    .iter()
                    .map(|row| match &row.cursor {
                        elephant_storage::canonical_extension::NativeExtensionValue::Thing(id) => {
                            id.clone()
                        }
                        _ => panic!("fingerprint physical identity is not a native record"),
                    })
                    .collect::<Vec<_>>();
                actual_ids.sort();
                actual_ids.dedup_by(|left, right| (*left).cmp(right).is_eq());
                assert_eq!(actual_ids, expected);
                ordinary_written += records.len();
                checked_tables.insert((*table).to_owned());
                eprintln!(
                    "representative native {table}: {} exact rows",
                    records.len()
                );
            }
            assert!(
                selected.ordinary.is_empty(),
                "every declared populated ordinary table must run"
            );
            assert_eq!(ordinary_written, 76);
            assert_eq!(vector_written, 2);
            assert_eq!(checked_tables.len(), 25);
            let receipt_id = selected.receipt["id"].as_str().unwrap();
            let history = streaming::fingerprints(&exclusive, "migration_history", None)
                .await
                .map_err(|error| StorageError::DataIntegrityError(error.to_string()))?;
            assert!(!history.iter().any(|row| row.id == receipt_id));
            let before_duplicate = native_digest(&exclusive).await?;
            assert!(
                import_embedding_batch(&exclusive, &bound, &manager, &selected.embeddings)
                    .await
                    .is_err()
            );
            assert_eq!(native_digest(&exclusive).await?, before_duplicate);
            exclusive.initialize_journal_identity().await?;
            Ok(())
        })
        .await
        .unwrap();
}
