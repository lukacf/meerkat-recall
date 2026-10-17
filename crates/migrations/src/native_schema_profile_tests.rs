//! Central-only pristine schema capture; emitted bytes are an unreviewed proposal.
//! No Source, document, space configuration, provider or erased fixture is seeded.
use super::{apply_schema, schema_files, verify_schema};
use elephant_storage::snapshot::{export_named, restore_named, SnapshotRetention};
use elephant_storage::snapshot_inspection::{
    inspect_snapshot, InspectedSnapshotIdentity, InspectedSnapshotRow, NativeInspectionValue,
    SnapshotInspectionLimits,
};
use elephant_storage::{StorageClient, StorageConfig};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{Error, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Info = BTreeMap<String, BTreeMap<String, String>>;
const MAX_PROFILE_BYTES: usize = 16 * 1024 * 1024;
const OUTPUT_ENV: &str = "RECALL_PRISTINE_SCHEMA_PROFILE_OUTPUT";

fn require(ok: bool, code: &'static str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(code.into())
    }
}

fn sha(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

struct OwnedDirectory(PathBuf);
impl OwnedDirectory {
    fn new() -> Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let parent = fs::canonicalize(std::env::temp_dir())?;
        let time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        for _ in 0..16 {
            let serial = NEXT.fetch_add(1, Ordering::Relaxed);
            let path = parent.join(format!(
                "recall-pristine-schema-{}-{time}-{serial}",
                std::process::id()
            ));
            match fs::DirBuilder::new().mode(0o700).create(&path) {
                Ok(()) => return Ok(Self(path)),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err("pristine_schema_scratch_collision".into())
    }
}
impl Drop for OwnedDirectory {
    fn drop(&mut self) {
        // This handle exists only after successful exclusive creation above.
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct Definition {
    table: Option<String>,
    category: String,
    name: String,
    statement: String,
    sha256: String,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct ExportStatement {
    ordinal: usize,
    statement: String,
    sha256: String,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[expect(
    clippy::struct_field_names,
    reason = "Preserve the serialized stage field in the existing pristine-schema profile format."
)]
struct Stage {
    stage: String,
    definitions: Vec<Definition>,
    export_schema_statements: Vec<ExportStatement>,
    intentionally_excluded_table: &'static str,
}

fn append_definitions(result: &mut Vec<Definition>, table: Option<&str>, info: Info) {
    for (category, definitions) in info {
        for (name, statement) in definitions {
            if table.is_none() && category == "tables" && name == "source_erasure_membership" {
                continue;
            }
            result.push(Definition {
                table: table.map(str::to_owned),
                category: category.clone(),
                name,
                sha256: sha(statement.as_bytes()),
                statement,
            });
        }
    }
}

async fn canonical_definitions(client: &StorageClient) -> Result<Vec<Definition>> {
    let mut response = client.query_checked("INFO FOR DB;").await?;
    let info: Info = response
        .take::<Option<Info>>(0)?
        .ok_or("pristine_schema_db_info_missing")?;
    let categories = [
        "accesses",
        "analyzers",
        "apis",
        "configs",
        "functions",
        "models",
        "params",
        "tables",
        "users",
    ];
    require(
        info.keys().map(String::as_str).eq(categories),
        "pristine_schema_unknown_db_category",
    )?;
    for category in ["accesses", "apis", "configs", "models", "params", "users"] {
        require(
            info.get(category).is_some_and(BTreeMap::is_empty),
            "pristine_schema_unexpected_sensitive_definition",
        )?;
    }
    let tables = info
        .get("tables")
        .ok_or("pristine_schema_tables_missing")?
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    let mut definitions = Vec::new();
    append_definitions(&mut definitions, None, info);
    for table in tables {
        if table == "source_erasure_membership" {
            continue;
        }
        require(
            !table.is_empty()
                && table
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "pristine_schema_unknown_table_identifier",
        )?;
        let mut response = client
            .query_checked(&format!("INFO FOR TABLE `{table}`;"))
            .await?;
        let info: Info = response
            .take::<Option<Info>>(0)?
            .ok_or("pristine_schema_table_info_missing")?;
        require(
            info.keys()
                .map(String::as_str)
                .eq(["events", "fields", "indexes", "lives", "tables"]),
            "pristine_schema_unknown_table_category",
        )?;
        require(
            info.get("lives").is_some_and(BTreeMap::is_empty),
            "pristine_schema_live_query_present",
        )?;
        append_definitions(&mut definitions, Some(&table), info);
        require(
            definitions.len() <= 4096,
            "pristine_schema_definition_bound",
        )?;
    }
    require(!definitions.is_empty(), "pristine_schema_empty_inventory")?;
    Ok(definitions)
}

// Migration 046 creates one global support control even in a pristine store.
// Admit only its exact native shape; no tenant epoch or payload is profile input.
fn pristine_support_control(row: &InspectedSnapshotRow) -> bool {
    let expected_id = InspectedSnapshotIdentity {
        table: "canonical_epoch".into(),
        id: NativeInspectionValue::Array(vec![
            NativeInspectionValue::String("recall_source_support_index".into()),
            NativeInspectionValue::Int(1),
        ]),
    };
    let space = "__recall_source_support_index__";
    let Some(NativeInspectionValue::Map(fields)) = &row.record else {
        return false;
    };
    let Some(NativeInspectionValue::String(nonce)) = fields.get("support_nonce") else {
        return false;
    };
    // The migration writes rand::uuid() as a canonical UUID string. Its value
    // stays private and is excluded from the deterministic emitted profile.
    let canonical_uuid = nonce.len() == 36
        && nonce.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        });
    row.identity == expected_id
        && row.space_id.as_deref() == Some(space)
        && fields.len() == 3
        && fields.get("id") == Some(&NativeInspectionValue::Thing(Box::new(expected_id)))
        && fields.get("space_id") == Some(&NativeInspectionValue::String(space.into()))
        && canonical_uuid
}

fn pristine_table_rows(table: &str, count: u64, rows: &[InspectedSnapshotRow]) -> bool {
    if table == "canonical_epoch" {
        let mut controls = rows.iter().filter(|row| row.identity.table == table);
        return count == 1
            && controls.next().is_some_and(pristine_support_control)
            && controls.next().is_none();
    }
    count == 0
        || matches!(
            table,
            "migration_history" | "recall_durability" | "publication_schema"
        )
}

async fn stage(client: &StorageClient, path: &Path, name: &str) -> Result<Stage> {
    let definitions = canonical_definitions(client).await?;
    export_named(client, path, name, &SnapshotRetention::default()).await?;
    let inspection = inspect_snapshot(
        path,
        &SnapshotInspectionLimits {
            include_records: true,
            ..SnapshotInspectionLimits::default()
        },
    )?;
    require(
        inspection.table_counts.get("canonical_epoch") == Some(&1),
        "pristine_schema_support_control_missing_or_extra",
    )?;
    for (table, count) in &inspection.table_counts {
        require(
            pristine_table_rows(table, *count, &inspection.rows),
            "pristine_schema_user_row_present",
        )?;
    }
    let mut statements = Vec::new();
    for (ordinal, definition) in inspection.schema_statements.into_iter().enumerate() {
        let statement = definition
            .statement
            .ok_or("pristine_schema_statement_missing")?;
        require(
            sha(statement.as_bytes()) == definition.sha256,
            "pristine_schema_statement_hash_mismatch",
        )?;
        statements.push(ExportStatement {
            ordinal,
            statement,
            sha256: definition.sha256,
        });
    }
    require(
        !statements.is_empty() && statements.len() <= 4096,
        "pristine_schema_export_inventory_bound",
    )?;
    // No inspection manifest, row, anchor, UUID, journal path or key is emitted.
    Ok(Stage {
        stage: name.into(),
        definitions,
        export_schema_statements: statements,
        intentionally_excluded_table: "source_erasure_membership",
    })
}

async fn capture_pristine() -> Result<Vec<Stage>> {
    let directory = OwnedDirectory::new()?;
    let client = Box::pin(StorageClient::new_with_durability(
        StorageConfig::default(),
        None,
        Some(directory.0.join("journal")),
    ))
    .await?;
    Box::pin(apply_schema(&client))
        .await
        .map_err(Error::other)?;
    require(
        schema_files().len() == 48,
        "pristine_schema_requires_final_001_through_048_registry",
    )?;
    let original = directory.0.join("original");
    let before = stage(&client, &original, "fresh-migrations-001-through-048").await?;
    // Ordinary restore performs the standard dynamic reference-guard refresh.
    // It uses this same pristine store's original journal; no key is synthesized.
    restore_named(&client, &original, client.erasure_journal()?).await?;
    verify_schema(&client).await.map_err(Error::other)?;
    let after = stage(
        &client,
        &directory.0.join("refreshed"),
        "standard-restore-refreshed",
    )
    .await?;
    drop(client);
    Ok(vec![before, after])
}

fn compiled_sources() -> BTreeMap<&'static str, String> {
    macro_rules! input {
        ($name:literal, $path:literal) => {
            ($name, sha(include_bytes!($path)))
        };
    }
    BTreeMap::from([
        input!("Cargo.lock", "../../../Cargo.lock"),
        input!("Cargo.toml", "../../../Cargo.toml"),
        input!("crates/migrations/src/lib.rs", "lib.rs"),
        input!(
            "crates/migrations/src/native_schema_profile_tests.rs",
            "native_schema_profile_tests.rs"
        ),
        input!(
            "crates/storage/src/client.rs",
            "../../storage/src/client.rs"
        ),
        input!(
            "crates/storage/src/reference_fence.rs",
            "../../storage/src/reference_fence.rs"
        ),
        input!(
            "crates/storage/src/snapshot.rs",
            "../../storage/src/snapshot.rs"
        ),
        input!(
            "crates/storage/src/snapshot_inspection.rs",
            "../../storage/src/snapshot_inspection.rs"
        ),
        input!(
            "crates/storage/src/snapshot_inspection_native.rs",
            "../../storage/src/snapshot_inspection_native.rs"
        ),
        input!(
            "vendor/surrealdb-core-2.6.5/Cargo.toml",
            "../../../vendor/surrealdb-core-2.6.5/Cargo.toml"
        ),
        input!(
            "vendor/surrealdb-core-2.6.5/src/kvs/export/v2.rs",
            "../../../vendor/surrealdb-core-2.6.5/src/kvs/export/v2.rs"
        ),
        input!(
            "vendor/surrealdb-core-2.6.5/src/sql/statements/info.rs",
            "../../../vendor/surrealdb-core-2.6.5/src/sql/statements/info.rs"
        ),
    ])
}

fn git(root: &Path, arguments: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .current_dir(root)
        .args(arguments)
        .output()?;
    require(
        output.status.success() && output.stdout.len() <= 16384,
        "pristine_schema_git_identity_failed",
    )?;
    Ok(String::from_utf8(output.stdout)?.trim_end().into())
}

fn write_profile(path: &Path, root: &Path, bytes: &[u8]) -> Result<()> {
    require(
        bytes.len() <= MAX_PROFILE_BYTES,
        "pristine_schema_profile_size_bound",
    )?;
    let parent = path
        .parent()
        .ok_or("pristine_schema_output_parent_missing")?;
    require(
        path.is_absolute() && fs::canonicalize(parent)? == parent && !path.starts_with(root),
        "pristine_schema_output_must_be_canonical_outside_checkout",
    )?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn migration_inventory(root: &Path) -> Result<Vec<serde_json::Value>> {
    let mut paths = fs::read_dir(root.join("crates/migrations/sql"))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.retain(|path| {
        path.extension()
            .is_some_and(|extension| extension == "surql")
    });
    paths.sort();
    require(
        paths.len() == 48 && paths.len() == schema_files().len(),
        "pristine_schema_runtime_migration_set_differs",
    )?;
    let mut inventory = Vec::new();
    for (index, (path, (name, compiled))) in paths.iter().zip(schema_files()).enumerate() {
        let filename = path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or("pristine_schema_migration_filename")?;
        require(
            filename.starts_with(&format!("{:03}_", index + 1)),
            "pristine_schema_migration_order_differs",
        )?;
        require(
            fs::read(path)? == compiled.as_bytes(),
            "pristine_schema_compiled_migration_differs",
        )?;
        inventory.push(serde_json::json!({"ordinal":index + 1,"name":name,
            "path":format!("crates/migrations/sql/{filename}"),"sha256":sha(compiled.as_bytes())}));
    }
    Ok(inventory)
}

#[test]
fn pristine_profile_output_is_exclusive_private_and_outside_checkout() -> Result<()> {
    use std::os::unix::fs::{symlink, PermissionsExt};
    let directory = OwnedDirectory::new()?;
    let root = directory.0.join("source");
    fs::create_dir(&root)?;
    let output = directory.0.join("profile.json");
    write_profile(&output, &root, b"original")?;
    require(
        fs::metadata(&output)?.permissions().mode() & 0o777 == 0o600,
        "pristine_schema_output_not_private",
    )?;
    require(
        write_profile(&output, &root, b"replacement").is_err(),
        "pristine_schema_overwrite_allowed",
    )?;
    require(
        fs::read(&output)? == b"original",
        "pristine_schema_original_replaced",
    )?;
    let link = directory.0.join("link.json");
    symlink(directory.0.join("absent.json"), &link)?;
    require(
        write_profile(&link, &root, b"symlink").is_err(),
        "pristine_schema_symlink_allowed",
    )?;
    require(
        write_profile(&root.join("forbidden.json"), &root, b"in source").is_err(),
        "pristine_schema_checkout_output_allowed",
    )?;
    require(
        write_profile(
            &directory.0.join("oversized.json"),
            &root,
            &vec![0; MAX_PROFILE_BYTES + 1],
        )
        .is_err(),
        "pristine_schema_oversized_output_allowed",
    )?;
    Ok(())
}

#[test]
fn pristine_profile_rejects_tenant_and_malformed_support_rows() {
    let identity = InspectedSnapshotIdentity {
        table: "canonical_epoch".into(),
        id: NativeInspectionValue::Array(vec![
            NativeInspectionValue::String("recall_source_support_index".into()),
            NativeInspectionValue::Int(1),
        ]),
    };
    let space = "__recall_source_support_index__";
    let fields = BTreeMap::from([
        (
            "id".into(),
            NativeInspectionValue::Thing(Box::new(identity.clone())),
        ),
        (
            "space_id".into(),
            NativeInspectionValue::String(space.into()),
        ),
        (
            "support_nonce".into(),
            NativeInspectionValue::String("00000000-0000-4000-8000-000000000001".into()),
        ),
    ]);
    let original = InspectedSnapshotRow {
        identity,
        space_id: Some(space.into()),
        sha256: String::new(),
        references: Vec::new(),
        record: Some(NativeInspectionValue::Map(fields.clone())),
    };
    assert!(pristine_table_rows(
        "canonical_epoch",
        1,
        std::slice::from_ref(&original)
    ));
    assert!(!pristine_table_rows("canonical_epoch", 0, &[]));
    assert!(!pristine_table_rows("canonical_epoch", 1, &[]));
    assert!(!pristine_table_rows(
        "canonical_epoch",
        2,
        &[original.clone(), original.clone()]
    ));
    assert!(!pristine_table_rows(
        "source",
        1,
        std::slice::from_ref(&original)
    ));
    for (key, value) in [
        (
            "epoch",
            NativeInspectionValue::String("tenant-epoch".into()),
        ),
        ("space_id", NativeInspectionValue::String("tenant".into())),
        (
            "support_nonce",
            NativeInspectionValue::String("not-a-uuid".into()),
        ),
        ("support_nonce", NativeInspectionValue::Null),
        (
            "support_nonce",
            NativeInspectionValue::Uuid("00000000-0000-4000-8000-000000000001".into()),
        ),
        (
            "id",
            NativeInspectionValue::String("canonical_epoch:tenant".into()),
        ),
    ] {
        let mut changed = original.clone();
        let mut payload = fields.clone();
        payload.insert(key.into(), value);
        changed.record = Some(NativeInspectionValue::Map(payload));
        assert!(
            !pristine_table_rows("canonical_epoch", 1, &[changed]),
            "{key}"
        );
    }
    let mut missing = original.clone();
    let mut payload = fields;
    payload.remove("support_nonce");
    missing.record = Some(NativeInspectionValue::Map(payload));
    assert!(!pristine_table_rows("canonical_epoch", 1, &[missing]));
    let mut tenant = original.clone();
    tenant.identity.id = NativeInspectionValue::String("tenant".into());
    assert!(!pristine_table_rows("canonical_epoch", 1, &[tenant]));
    let mut wrong_space = original;
    wrong_space.space_id = Some("tenant".into());
    assert!(!pristine_table_rows("canonical_epoch", 1, &[wrong_space]));
}

#[tokio::test]
async fn pristine_native_schema_profile_matches_independent_instances() -> Result<()> {
    let first =
        tokio::time::timeout(Duration::from_secs(60), Box::pin(capture_pristine())).await??;
    let second =
        tokio::time::timeout(Duration::from_secs(60), Box::pin(capture_pristine())).await??;
    require(
        first == second,
        "pristine_schema_independent_instances_differ",
    )?;
    if let Some(output) = std::env::var_os(OUTPUT_ENV) {
        let root = fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))?;
        require(
            git(
                &root,
                &["status", "--porcelain", "--untracked-files=normal"],
            )?
            .is_empty(),
            "pristine_schema_checkout_not_clean",
        )?;
        let inputs = compiled_sources();
        for (relative, expected) in &inputs {
            require(
                sha(&fs::read(root.join(relative))?) == *expected,
                "pristine_schema_compiled_source_differs",
            )?;
        }
        let migrations = migration_inventory(&root)?;
        let profile = serde_json::json!({
            "format":"recall-pristine-native-schema-profile/v1",
            "review_state":"unreviewed-proposal", "product_acceptance":false,
            "execution_authorized":false, "source_commit":git(&root, &["rev-parse","HEAD"])?,
            "source_tree":git(&root, &["rev-parse","HEAD^{tree}"])?,
            "engine_source_tree":git(&root, &["rev-parse","HEAD:vendor/surrealdb-core-2.6.5"])?,
            "compiled_source_sha256":inputs, "ordered_migrations":migrations,
            "instances":2, "independent_instances_equal":true, "stages":first,
            "proof_limits":["pristine capture is not an independent allowed-schema verdict",
                "each definition must be mapped to pinned source and independently reviewed",
                "unknown candidate definitions remain unmet; equality across candidate exports is insufficient",
                "no erasure, payload absence, provider, restore recovery or product acceptance from this profile"]
        });
        write_profile(
            &PathBuf::from(output),
            &root,
            &serde_json::to_vec_pretty(&profile)?,
        )?;
    }
    Ok(())
}
