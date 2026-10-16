//! Native readiness/recovery nonce event boundaries on fresh and populated stores.
//!
//! Direct writes below isolate the database event, including unknown fields that
//! domain DTOs cannot yet express. The pipeline's concurrent finalizer regression
//! covers the actual readiness and guarded publication paths; these controls do
//! not claim provider behavior or whole-source completion.

#![allow(
    clippy::significant_drop_tightening,
    reason = "Keep the original fixture client and observations through each persisted-state assertion"
)]

use std::collections::BTreeMap;

use elephant_storage::canonical_commit::CanonicalRepository;
use elephant_storage::{StorageClient, StorageError};
use serde::Deserialize;
use serde_json::json;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const SPACE: &str = "read-nonce-original-space";
const OTHER_SPACE: &str = "read-nonce-other-space";
const TABLES: [&str; 3] = ["ingest_decision", "source_revision", "source_head"];
const MIGRATION: &str = "canonical_read_semantic_epoch";

fn bindings(table: &str) -> serde_json::Value {
    json!({"table": table, "key": "read_nonce_probe", "space": SPACE, "other": OTHER_SPACE})
}

async fn epochs(client: &StorageClient) -> TestResult<[String; 2]> {
    let canonical = CanonicalRepository::new(client.clone());
    Ok([
        canonical.current_epoch(SPACE).await?,
        canonical.current_epoch(OTHER_SPACE).await?,
    ])
}

async fn seed(client: &StorageClient) -> TestResult {
    client.initialize_journal_identity().await?;
    let canonical = CanonicalRepository::new(client.clone());
    canonical.epoch(SPACE).await?;
    canonical.epoch(OTHER_SPACE).await?;
    for table in TABLES {
        mutate(
            client,
            table,
            "CREATE type::thing($table,$key) SET space_id=$space, \
             source_id='source:nonce-probe',payload='original';",
            [true, false],
        )
        .await?;
    }
    Ok(())
}

async fn mutate(client: &StorageClient, table: &str, sql: &str, changed: [bool; 2]) -> TestResult {
    let before = epochs(client).await?;
    client.query_bind_checked(sql, bindings(table)).await?;
    let after = epochs(client).await?;
    for (index, space) in [SPACE, OTHER_SPACE].iter().enumerate() {
        assert_eq!(
            before[index] != after[index],
            changed[index],
            "epoch boundary: {table} in {space}: {sql}"
        );
    }
    Ok(())
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct ProbeDb {
    space_id: String,
    payload: String,
    canonical_read_nonce: Option<String>,
    fence_nonce: Option<String>,
}

async fn probe(client: &StorageClient, table: &str) -> TestResult<ProbeDb> {
    let mut response = client
        .query_bind_checked(
            "SELECT space_id,payload,canonical_read_nonce,fence_nonce \
             FROM type::thing($table,$key);",
            bindings(table),
        )
        .await?;
    let rows: Vec<ProbeDb> = response.take(0)?;
    assert_eq!(rows.len(), 1, "one original physical probe row");
    rows.into_iter()
        .next()
        .ok_or_else(|| "probe missing".into())
}

#[derive(Deserialize)]
struct RevisionDb {
    revision: u64,
}

async fn reference_revision(client: &StorageClient) -> TestResult<u64> {
    let mut response = client
        .query_bind_checked(
            "SELECT revision FROM type::thing('source_erasure_guard',$space);",
            json!({"space": SPACE}),
        )
        .await?;
    let rows: Vec<RevisionDb> = response.take(0)?;
    assert_eq!(rows.len(), 1, "original shared reference guard exists");
    Ok(rows
        .into_iter()
        .next()
        .ok_or("reference guard missing")?
        .revision)
}

#[derive(Deserialize)]
struct PhysicalDb {
    nonce: String,
}

async fn physical_nonce(client: &StorageClient) -> TestResult<String> {
    let mut response = client
        .query_bind_checked(
            "SELECT nonce FROM type::thing('native_read_fence', \
             string::concat('source_head:',string::slice(crypto::sha256( \
             type::string(type::thing('source_head',$key))),0,3)));",
            bindings("source_head"),
        )
        .await?;
    let rows: Vec<PhysicalDb> = response.take(0)?;
    assert_eq!(rows.len(), 1, "original head physical bucket exists");
    Ok(rows
        .into_iter()
        .next()
        .ok_or("physical bucket missing")?
        .nonce)
}

async fn nonce_boundaries(client: &StorageClient) -> TestResult {
    for table in TABLES {
        // Start with no nonce on fresh and upgraded fixtures alike. The actual
        // persisted field must change, even though canonical epochs do not.
        for value in [None, Some("first"), Some("second"), None] {
            let before_reference = reference_revision(client).await?;
            let before_physical = physical_nonce(client).await?;
            let before_row = probe(client, table).await?;
            let (sql, expected) = match value {
                Some("first") => (
                    "UPDATE type::thing($table,$key) SET canonical_read_nonce='first';",
                    Some("first"),
                ),
                Some(_) => (
                    "UPDATE type::thing($table,$key) SET canonical_read_nonce='second';",
                    Some("second"),
                ),
                None => (
                    "UPDATE type::thing($table,$key) UNSET canonical_read_nonce;",
                    None,
                ),
            };
            mutate(client, table, sql, [false, false]).await?;
            let row = probe(client, table).await?;
            assert_eq!(row.canonical_read_nonce.as_deref(), expected);
            assert_eq!(row.space_id, before_row.space_id);
            assert_eq!(row.payload, before_row.payload);
            // A no-op UNSET may be optimized away. Every actual nonce mutation
            // must retain the original shared reference conflict write, and
            // head mutations additionally retain the physical identity bucket.
            if row.canonical_read_nonce != before_row.canonical_read_nonce {
                assert!(reference_revision(client).await? > before_reference);
                if table == "source_head" {
                    assert_ne!(physical_nonce(client).await?, before_physical);
                }
            }
        }
    }
    for (sql, canonical, fence) in [
        (
            "UPDATE type::thing($table,$key) SET fence_nonce='first';",
            None,
            Some("first"),
        ),
        (
            "UPDATE type::thing($table,$key) SET canonical_read_nonce='both',fence_nonce='both';",
            Some("both"),
            Some("both"),
        ),
        (
            "UPDATE type::thing($table,$key) UNSET fence_nonce;",
            Some("both"),
            None,
        ),
        (
            "UPDATE type::thing($table,$key) UNSET canonical_read_nonce;",
            None,
            None,
        ),
    ] {
        let before_reference = reference_revision(client).await?;
        let before_physical = physical_nonce(client).await?;
        mutate(client, "source_head", sql, [false, false]).await?;
        let row = probe(client, "source_head").await?;
        assert_eq!(row.canonical_read_nonce.as_deref(), canonical);
        assert_eq!(row.fence_nonce.as_deref(), fence);
        assert!(reference_revision(client).await? > before_reference);
        assert_ne!(physical_nonce(client).await?, before_physical);
    }
    Ok(())
}

async fn semantic_boundaries(client: &StorageClient) -> TestResult {
    for table in TABLES {
        for sql in [
            "UPDATE type::thing($table,$key) SET payload='changed',canonical_read_nonce='content';",
            "UPDATE type::thing($table,$key) SET security={ level: 3 },canonical_read_nonce='policy';",
            "UPDATE type::thing($table,$key) SET future_field='unknown',canonical_read_nonce='future';",
            "UPDATE type::thing($table,$key) SET future_field=NULL;",
            "UPDATE type::thing($table,$key) UNSET future_field;",
            // Explicit null is present: do not coalesce absent and null fields.
            "UPDATE type::thing($table,$key) SET future_field=NULL;",
            "UPDATE type::thing($table,$key) SET canonical_read_nonce_extra='not-exempt';",
            "UPDATE type::thing($table,$key) SET state={generation:'new',desired_revision:'r2',lifecycle:'retracted'};",
        ] {
            mutate(client, table, sql, [true, false]).await?;
        }
        if table != "source_head" {
            mutate(
                client,
                table,
                "UPDATE type::thing($table,$key) SET fence_nonce='head-only-exemption';",
                [true, false],
            )
            .await?;
        }
        mutate(
            client,
            table,
            "UPDATE type::thing($table,$key) SET space_id=$other,canonical_read_nonce='move';",
            [true, true],
        )
        .await?;
        mutate(
            client,
            table,
            "UPDATE type::thing($table,$key) SET canonical_read_nonce='other-space';",
            [false, false],
        )
        .await?;
        mutate(
            client,
            table,
            "DELETE type::thing($table,$key);",
            [false, true],
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn native_read_nonces_fresh_schema_preserves_semantics_and_physical_fences() -> TestResult {
    let client = StorageClient::new_memory().await?;
    Box::pin(super::apply_schema(&client)).await?;
    super::verify_schema(&client).await?;
    seed(&client).await?;
    nonce_boundaries(&client).await?;
    semantic_boundaries(&client).await?;
    Ok(())
}

#[tokio::test]
async fn native_read_nonces_populated_044_requires_explicit_upgrade() -> TestResult {
    let client = StorageClient::new_memory().await?;
    let files = super::schema_files();
    let index = files
        .iter()
        .position(|(name, _)| *name == MIGRATION)
        .ok_or("045 missing")?;
    assert_eq!(index, 44);
    assert_eq!(files[index - 1].0, "source_head_semantic_epoch");
    Box::pin(super::apply_files(
        &client,
        &files[..index],
        &BTreeMap::new(),
    ))
    .await?;
    seed(&client).await?;
    for table in TABLES {
        // Exercise the actual historical event before replacing it. 044's
        // head fence_nonce exemption does not cover canonical_read_nonce.
        mutate(
            &client,
            table,
            "UPDATE type::thing($table,$key) SET canonical_read_nonce='before-upgrade';",
            [true, false],
        )
        .await?;
    }
    let old_receipts = super::schema_receipts(&client).await?;
    let before_epoch = epochs(&client).await?;
    let mut old_tables = client.list_tables().await?;
    old_tables.sort_unstable();
    assert!(matches!(super::verify_schema(&client).await, Err(error) if error.contains(MIGRATION)));
    assert_eq!(super::schema_receipts(&client).await?, old_receipts);
    let mut refused_tables = client.list_tables().await?;
    refused_tables.sort_unstable();
    assert_eq!(
        refused_tables, old_tables,
        "verification never silently applies schema"
    );
    assert_eq!(epochs(&client).await?, before_epoch);

    Box::pin(super::apply_schema(&client)).await?;
    super::verify_schema(&client).await?;
    let receipts = super::schema_receipts(&client).await?;
    assert_eq!(
        receipts.len(),
        files.len(),
        "every registered migration applied"
    );
    for (name, sql) in &files[index..] {
        assert_eq!(
            receipts.get(*name),
            Some(&super::checksum(sql)),
            "each pending migration has its exact registered checksum"
        );
    }
    for (name, checksum) in old_receipts {
        assert_eq!(
            receipts.get(&name),
            Some(&checksum),
            "historical SQL unchanged"
        );
    }
    assert_eq!(
        receipts.get(MIGRATION),
        Some(&super::checksum(files[index].1))
    );
    assert_eq!(
        epochs(&client).await?,
        before_epoch,
        "upgrade does not rewrite input data"
    );
    for table in TABLES {
        assert_eq!(
            probe(&client, table).await?.canonical_read_nonce.as_deref(),
            Some("before-upgrade")
        );
    }
    nonce_boundaries(&client).await?;
    semantic_boundaries(&client).await?;
    Box::pin(super::apply_schema(&client)).await?;
    assert_eq!(super::schema_receipts(&client).await?, receipts);
    client
        .query_bind_checked(
            "UPDATE migration_history SET checksum='wrong' WHERE name=$name;",
            json!({"name": MIGRATION}),
        )
        .await?;
    let damaged = super::schema_receipts(&client).await?;
    assert!(super::verify_schema(&client).await.is_err());
    assert!(Box::pin(super::apply_schema(&client)).await.is_err());
    assert_eq!(super::schema_receipts(&client).await?, damaged);
    Ok(())
}

#[tokio::test]
async fn native_read_nonces_still_refuse_original_erasure_reservation() -> TestResult {
    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct ReservationDb {
        space_id: String,
        erasure_reservation: String,
    }

    let client = StorageClient::new_memory().await?;
    Box::pin(super::apply_schema(&client)).await?;
    seed(&client).await?;
    client.query_bind_checked(
        "UPDATE type::thing('source_erasure_guard',$space) SET erasure_reservation='other-owner';",
        json!({"space": SPACE}),
    ).await?;
    let mut response = client
        .query_bind_checked(
            "SELECT space_id,erasure_reservation FROM type::thing('source_erasure_guard',$space);",
            json!({"space": SPACE}),
        )
        .await?;
    let reservations: Vec<ReservationDb> = response.take(0)?;
    assert_eq!(
        reservations,
        vec![ReservationDb {
            space_id: SPACE.to_owned(),
            erasure_reservation: "other-owner".to_owned(),
        }],
        "the original space guard must hold the competing reservation before any nonce write"
    );
    for table in TABLES {
        let before = probe(&client, table).await?;
        let before_epoch = epochs(&client).await?;
        let before_reference = reference_revision(&client).await?;
        let before_physical = physical_nonce(&client).await?;
        let result = client
            .query_bind_checked(
                "UPDATE type::thing($table,$key) SET canonical_read_nonce='must-refuse';",
                bindings(table),
            )
            .await;
        // Checked queries expose the typed reservation refusal, not the native
        // THROW sentinel. Any other error (or a successful write) is a failure.
        let error = result
            .err()
            .ok_or_else(|| format!("reserved nonce write unexpectedly succeeded for {table}"))?;
        assert!(
            matches!(&error, StorageError::ErasureInProgress),
            "{table}: expected ErasureInProgress, got {error:?}"
        );
        assert_eq!(probe(&client, table).await?, before);
        assert_eq!(epochs(&client).await?, before_epoch);
        assert_eq!(reference_revision(&client).await?, before_reference);
        assert_eq!(physical_nonce(&client).await?, before_physical);
    }
    Ok(())
}
