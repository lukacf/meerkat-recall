//! Synthetic lexical-spool boundary controls; no household snapshot.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use elephant_types::records::{BaseFields, Doc};
use elephant_types::{RecordId, TableName};

fn doc(text: String) -> serde_json::Value {
    serde_json::to_value(Doc {
        base: BaseFields::new(
            RecordId::new_v4(TableName::Doc),
            "synthetic".into(),
            "original".into(),
        ),
        title: "synthetic".into(),
        text,
        sidecar_text: None,
        source: None,
        media_attachments: vec![],
        rendition_key: uuid::Uuid::new_v4().to_string(),
        metadata: serde_json::json!({"integer":9_007_199_254_740_993_i64,"float":1.0}),
    })
    .unwrap()
}
fn fixture(root: &Path, rows: Vec<(String, String)>, mut tables: BTreeMap<String, u64>) -> PathBuf {
    let index = root.join("inventory.sqlite");
    let mut db = Connection::open(&index).unwrap();
    db.execute_batch("CREATE TABLE rows(t TEXT,k TEXT,h TEXT,space TEXT,deleted INTEGER,PRIMARY KEY(t,k)); CREATE TABLE raw_rows(t TEXT,k TEXT,lexical BLOB,lexical_sha256 TEXT,PRIMARY KEY(t,k)); CREATE TABLE metadata(k TEXT PRIMARY KEY,v TEXT); CREATE TABLE reference_candidates(row_table TEXT,row_id TEXT,path TEXT,ref_table TEXT,ref_id TEXT,PRIMARY KEY(row_table,row_id,path,ref_table,ref_id));").unwrap();
    let tx = db.transaction().unwrap();
    for (table, raw) in rows {
        let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
        tx.execute(
            "INSERT INTO rows VALUES (?,?,?,?,?)",
            params![
                table,
                value["id"].as_str().unwrap(),
                "b".repeat(64),
                value["space_id"].as_str(),
                0
            ],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO raw_rows VALUES (?,?,?,?)",
            params![
                table,
                value["id"].as_str().unwrap(),
                raw.as_bytes(),
                hex::encode(Sha256::digest(raw.as_bytes()))
            ],
        )
        .unwrap();
        *tables.entry(table).or_default() += 1;
    }
    let mut plan = serde_json::json!({"format":"recall-preservation-plan-v1","spool_schema":"recall-original-lexical-row-spool-v1","state":"offline_inventory_only","publication_certified":false,"space_id":"synthetic","canonical_hash_contract":{"id":"homecore-python-json-v1"},"snapshot_header":{"backend":"elephant-db","version":1,"exported_at":"2026-10-10T00:00:00Z"},"snapshot_sha256":"a".repeat(64),"plan_id":"c".repeat(64),"tables":tables});
    for key in [
        "format",
        "spool_schema",
        "canonical_hash_contract",
        "snapshot_header",
        "snapshot_sha256",
        "plan_id",
        "space_id",
        "tables",
    ] {
        tx.execute(
            "INSERT INTO metadata VALUES (?,?)",
            params![key, plan[key].to_string()],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    db.close().unwrap();
    plan["inventory_sha256"] = file_sha(&index).unwrap().into();
    let path = root.join("plan.json");
    std::fs::write(&path, serde_json::to_vec(&plan).unwrap()).unwrap();
    path
}
fn rebind(root: &Path) {
    let path = root.join("plan.json");
    let mut plan: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    plan["inventory_sha256"] = file_sha(&root.join("inventory.sqlite")).unwrap().into();
    std::fs::write(path, serde_json::to_vec(&plan).unwrap()).unwrap();
}

#[test]
fn bounded_spool_pages_all_rows_and_preserves_explicit_empty_tables() {
    let dir = tempfile::tempdir().unwrap();
    let rows = (0..513)
        .map(|i| ("doc".into(), doc(format!("synthetic {i}")).to_string()))
        .collect();
    let path = fixture(dir.path(), rows, BTreeMap::from([("audit_log".into(), 0)]));
    let spool = SourceSpool::open(&path, &dir.path().join("checks.sqlite")).unwrap();
    assert_eq!(spool.tables["audit_log"], 0);
    assert_eq!(spool.tables["doc"], 513);
    let mut cursor = None;
    let mut ids = BTreeSet::new();
    let mut lengths = vec![];
    loop {
        let page = spool.page("doc", cursor.as_deref()).unwrap();
        if page.is_empty() {
            break;
        }
        lengths.push(page.len());
        for row in &page {
            assert!(ids.insert(row.id.clone()));
        }
        cursor = page.last().map(|r| r.id.clone());
    }
    assert_eq!(lengths, vec![256, 256, 1]);
    assert_eq!(ids.len(), 513);
    spool.verify_unchanged().unwrap();
}

#[test]
fn twelve_mib_document_is_one_bounded_page_without_threefold_estimate_rejection() {
    let dir = tempfile::tempdir().unwrap();
    let raw = doc("x".repeat(12 * 1024 * 1024)).to_string();
    let path = fixture(
        dir.path(),
        vec![("doc".into(), raw.clone())],
        BTreeMap::new(),
    );
    let spool = SourceSpool::open(&path, &dir.path().join("checks.sqlite")).unwrap();
    let page = spool.page("doc", None).unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].lexical, raw);
}

#[test]
fn spool_refuses_tamper_missing_lexical_duplicate_key_and_unknown_field() {
    for mode in 0..5 {
        let dir = tempfile::tempdir().unwrap();
        let raw = doc("synthetic".into()).to_string();
        let path = fixture(
            dir.path(),
            vec![("doc".into(), raw.clone())],
            BTreeMap::new(),
        );
        let db = Connection::open(dir.path().join("inventory.sqlite")).unwrap();
        match mode {
            0 => {
                db.execute("UPDATE rows SET h=?", ["d".repeat(64)]).unwrap();
            }
            1 => {
                db.execute("DELETE FROM raw_rows", []).unwrap();
            }
            2 | 3 => {
                let replacement = if mode == 2 {
                    format!("{{\"text\":\"duplicate\",{}", &raw[1..])
                } else {
                    format!("{{\"unknown\":true,{}", &raw[1..])
                };
                db.execute(
                    "UPDATE raw_rows SET lexical=?,lexical_sha256=?",
                    params![
                        replacement.as_bytes(),
                        hex::encode(Sha256::digest(replacement.as_bytes()))
                    ],
                )
                .unwrap();
            }
            _ => {
                db.execute("UPDATE raw_rows SET lexical_sha256=?", ["d".repeat(64)])
                    .unwrap();
            }
        }
        db.close().unwrap();
        if mode != 0 {
            rebind(dir.path());
        }
        assert!(
            SourceSpool::open(&path, &dir.path().join("checks.sqlite")).is_err(),
            "mutation {mode}"
        );
    }
}

#[test]
fn spool_preflights_oversized_row_without_allocating_lexical_blob() {
    let dir = tempfile::tempdir().unwrap();
    let path = fixture(
        dir.path(),
        vec![("doc".into(), doc("synthetic".into()).to_string())],
        BTreeMap::new(),
    );
    let db = Connection::open(dir.path().join("inventory.sqlite")).unwrap();
    db.execute(
        "UPDATE raw_rows SET lexical=zeroblob(?)",
        [(MAX_ROW_BYTES + 1) as u64],
    )
    .unwrap();
    db.close().unwrap();
    rebind(dir.path());
    let error = SourceSpool::open(&path, &dir.path().join("checks.sqlite"))
        .err()
        .unwrap();
    assert!(error.to_string().contains("single spool row exceeds"));
}

#[test]
fn spool_rejects_held_wal_and_sidecars_outside_inventory_hash() {
    let dir = tempfile::tempdir().unwrap();
    let path = fixture(
        dir.path(),
        vec![("doc".into(), doc("original".into()).to_string())],
        BTreeMap::new(),
    );
    let inventory = dir.path().join("inventory.sqlite");
    let writer = Connection::open(&inventory).unwrap();
    writer
        .execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
        .unwrap();
    // Bind the main file after the mode change. The uncheckpointed next write
    // remains outside that digest but is visible to an ordinary readonly reader.
    rebind(dir.path());
    let bound = file_sha(&inventory).unwrap();
    writer
        .execute("UPDATE rows SET h=?", ["d".repeat(64)])
        .unwrap();
    assert_eq!(file_sha(&inventory).unwrap(), bound);
    let ordinary =
        Connection::open_with_flags(&inventory, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let observed: String = ordinary
        .query_row("SELECT h FROM rows", [], |r| r.get(0))
        .unwrap();
    assert_eq!(observed, "d".repeat(64));
    assert!(SourceSpool::open(&path, &dir.path().join("wal-checks.sqlite")).is_err());
    drop(ordinary);
    writer
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;")
        .unwrap();
    writer.close().unwrap();
    rebind(dir.path());
    let spool = SourceSpool::open(&path, &dir.path().join("closed-checks.sqlite")).unwrap();
    for suffix in ["-wal", "-shm", "-journal"] {
        let sidecar = dir.path().join(format!("inventory.sqlite{suffix}"));
        std::fs::write(&sidecar, b"unbound").unwrap();
        assert!(spool.page("doc", None).is_err());
        assert!(spool.verify_unchanged().is_err());
        std::fs::remove_file(sidecar).unwrap();
    }
    spool.verify_unchanged().unwrap();
}

#[test]
fn spool_preserves_other_spaces_and_refuses_indexed_space_reassignment() {
    let dir = tempfile::tempdir().unwrap();
    let original = doc("other space original".into());
    let mut other = doc("retained foreign space".into());
    other["space_id"] = "other".into();
    let path = fixture(
        dir.path(),
        vec![
            ("doc".into(), original.to_string()),
            ("doc".into(), other.to_string()),
        ],
        BTreeMap::new(),
    );
    let spool = SourceSpool::open(&path, &dir.path().join("checks.sqlite")).unwrap();
    assert_eq!(spool.space_id, "synthetic");
    assert_eq!(
        spool.spaces,
        BTreeSet::from(["synthetic".into(), "other".into()])
    );
    let actual: Vec<serde_json::Value> = spool
        .page("doc", None)
        .unwrap()
        .iter()
        .map(|row| strict_json(&row.lexical).unwrap())
        .collect();
    assert!(actual.contains(&original));
    assert!(actual.contains(&other));
    let db = Connection::open(dir.path().join("inventory.sqlite")).unwrap();
    db.execute("UPDATE rows SET space='synthetic' WHERE space='other'", [])
        .unwrap();
    db.close().unwrap();
    rebind(dir.path());
    assert!(SourceSpool::open(&path, &dir.path().join("tampered-checks.sqlite")).is_err());
}

#[test]
fn known_legacy_empty_declarations_are_preserved_but_nonempty_and_unknown_refuse() {
    for table in LEGACY_EMPTY_TABLES {
        let dir = tempfile::tempdir().unwrap();
        let path = fixture(
            dir.path(),
            Vec::new(),
            BTreeMap::from([(table.to_string(), 0)]),
        );
        let spool = SourceSpool::open(&path, &dir.path().join("checks.sqlite")).unwrap();
        assert_eq!(spool.tables[*table], 0);
        assert!(spool.page(table, None).unwrap().is_empty());
        let path = dir.path().join("plan.json");
        let mut plan: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        plan["tables"][*table] = 1.into();
        std::fs::write(&path, serde_json::to_vec(&plan).unwrap()).unwrap();
        assert!(SourceSpool::open(&path, &dir.path().join("nonempty-checks.sqlite")).is_err());
    }
    let dir = tempfile::tempdir().unwrap();
    let path = fixture(
        dir.path(),
        Vec::new(),
        BTreeMap::from([("unknown_empty".into(), 0)]),
    );
    assert!(SourceSpool::open(&path, &dir.path().join("checks.sqlite")).is_err());
}

#[test]
fn actual_python_planner_spool_preserves_omitted_and_null_original_documents() {
    let dir = tempfile::tempdir().unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let fixture_path = root.join("scripts/migration/fixtures/python-raw-documents-v1.json");
    let code = r"
import sys, json
from pathlib import Path
sys.path.insert(0, str(Path(sys.argv[1]) / 'scripts/migration'))
import preservation_plan as p
root = Path(sys.argv[2])
fixture = json.loads(Path(sys.argv[3]).read_text())
rows = [case['original'] for case in fixture['cases']]
snapshot = root / 'snapshot.json'
idmap = root / 'idmap.jsonl'
snapshot.write_bytes(p.canonical({'backend':'elephant-db','version':1,'exported_at':'2026-10-10T00:00:00Z','tables':{'doc':rows}}))
idmap.write_bytes(b''.join(p.canonical({'old_doc_id':doc['id'],'class':'source_less','content_sha256':p.value_hash({'title':doc['title'],'text':doc['text']})}) + b'\n' for doc in rows))
plan = p.inventory(snapshot, idmap, root / 'plan', space='history-test', snapshot_sha256=p.file_sha(snapshot), idmap_sha256=p.file_sha(idmap))
for field in ['snapshot_sha256','idmap_sha256','documents_sha256']:
    assert plan[field] == fixture[field]
";
    let output = std::process::Command::new("python3")
        .arg("-c")
        .arg(code)
        .arg(&root)
        .arg(dir.path())
        .arg(&fixture_path)
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "synthetic planner refused: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let spool = SourceSpool::open(
        &dir.path().join("plan/plan.json"),
        &dir.path().join("checks.sqlite"),
    )
    .unwrap();
    let fixture: serde_json::Value =
        serde_json::from_slice(&std::fs::read(fixture_path).unwrap()).unwrap();
    let rows = spool.page("doc", None).unwrap();
    assert_eq!(rows.len(), 2);
    for case in fixture["cases"].as_array().unwrap() {
        let row = rows
            .iter()
            .find(|r| r.id == case["original"]["id"].as_str().unwrap())
            .unwrap();
        assert_eq!(row.lexical, case["original_lexical"].as_str().unwrap());
        assert_eq!(
            row.original_sha256,
            case["declaration"]["original_row_sha256"].as_str().unwrap()
        );
        assert!(matches!(
            typed("doc", &row.lexical).unwrap(),
            TypedRow::Ordinary(TransferRecord::Doc(_))
        ));
        for mode in 0..5 {
            let mut bad = case["original"].clone();
            match mode {
                0 => bad["provenance"]["unexpected"] = true.into(),
                1 => bad["security"]["unexpected"] = true.into(),
                2 => bad["created_at"] = "2026-10-10T00:00:00+00:00".into(),
                3 => {
                    bad.as_object_mut().unwrap().remove("created_at");
                }
                _ => bad["unknown_field"] = serde_json::Value::Null,
            }
            assert!(
                typed("doc", &bad.to_string()).is_err(),
                "unapproved representation {mode}"
            );
        }
    }
    spool.verify_unchanged().unwrap();
}

fn declare_mapping(root: &Path, source: &str, target: &str) {
    let path = root.join("plan.json");
    let mut plan: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let db = Connection::open(root.join("inventory.sqlite")).unwrap();
    for (field, value) in [("source_space_id", source), ("target_space_id", target)] {
        plan[field] = value.into();
        db.execute(
            "INSERT INTO metadata VALUES (?,?)",
            params![field, plan[field].to_string()],
        )
        .unwrap();
    }
    db.close().unwrap();
    std::fs::write(path, serde_json::to_vec(&plan).unwrap()).unwrap();
    rebind(root);
}

#[test]
#[expect(
    clippy::panic,
    reason = "A wrong projected fixture variant is an assertion failure; retain exact document-only branch and original bytes checks."
)]
fn explicit_space_projection_preserves_original_bytes_foreign_rows_and_policy() {
    let dir = tempfile::tempdir().unwrap();
    let mut original = doc("original".into());
    original["metadata"] = serde_json::json!({"space_id":"synthetic","unchanged":"synthetic"});
    let mut foreign = doc("foreign".into());
    foreign["space_id"] = "other".into();
    let path = fixture(
        dir.path(),
        vec![
            ("doc".into(), original.to_string()),
            ("doc".into(), foreign.to_string()),
        ],
        BTreeMap::new(),
    );
    declare_mapping(dir.path(), "synthetic", "destination");
    let spool = SourceSpool::open(&path, &dir.path().join("checks.sqlite")).unwrap();
    assert_eq!(
        spool.projected_spaces(),
        BTreeSet::from(["destination".into(), "other".into()])
    );
    for row in spool.page("doc", None).unwrap() {
        let original_row = strict_json(&row.lexical).unwrap();
        let TypedRow::Ordinary(projected) = spool.projected("doc", &row.lexical).unwrap() else {
            panic!("doc type");
        };
        let mut expected = original_row.clone();
        if expected["space_id"] == "synthetic" {
            expected["space_id"] = "destination".into();
        }
        assert_eq!(projected.json().unwrap(), expected);
        assert_eq!(strict_json(&row.lexical).unwrap(), original_row);
    }
    spool.verify_unchanged().unwrap();
}

#[test]
fn relocation_refuses_target_collision_and_foreign_reference_before_target_open() {
    for mode in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let original = doc("original".into());
        let mut foreign = doc("foreign".into());
        foreign["space_id"] = if mode == 0 { "destination" } else { "other" }.into();
        let path = fixture(
            dir.path(),
            vec![
                ("doc".into(), original.to_string()),
                ("doc".into(), foreign.to_string()),
            ],
            BTreeMap::new(),
        );
        if mode != 0 {
            let db = Connection::open(dir.path().join("inventory.sqlite")).unwrap();
            let (owner, target) = if mode == 1 {
                (&original, &foreign)
            } else {
                (&foreign, &original)
            };
            db.execute(
                "INSERT INTO reference_candidates VALUES ('doc',?,'/metadata/ref','doc',?)",
                params![
                    owner["id"].as_str().unwrap(),
                    target["id"].as_str().unwrap()
                ],
            )
            .unwrap();
            db.close().unwrap();
        }
        declare_mapping(dir.path(), "synthetic", "destination");
        assert!(
            SourceSpool::open(&path, &dir.path().join("checks.sqlite")).is_err(),
            "collision/reference {mode}"
        );
    }
}

#[test]
#[expect(
    clippy::panic,
    reason = "A non-embedding typed fixture variant must fail this exact precision and signed-zero control."
)]
fn vector_precision_preserves_native_f32_expansion_and_signed_zero() {
    use elephant_types::{records::NodeEmbedding, Link};
    let original = vec![0.1_f32, -0.0_f32, f32::from_bits(1), 1.0_f32];
    let row = NodeEmbedding {
        base: BaseFields::new(
            RecordId::new_v4(TableName::ChunkEmbedding),
            "synthetic".into(),
            "original".into(),
        ),
        node: Link::new(RecordId::new_v4(TableName::DocNode)),
        model: "synthetic".into(),
        dims: 4,
        vector: original.clone(),
    };
    let mut raw = serde_json::to_value(row).unwrap();
    raw["vector"] =
        serde_json::to_value(original.iter().copied().map(f64::from).collect::<Vec<_>>()).unwrap();
    let TypedRow::Embedding(actual) = typed("chunk_embedding", &raw.to_string()).unwrap() else {
        panic!("embedding")
    };
    assert_eq!(
        actual
            .vector
            .iter()
            .map(|v| v.to_bits())
            .collect::<Vec<_>>(),
        original.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
    );
    for value in [
        serde_json::json!(0.1_f64),
        serde_json::json!(1e-50),
        serde_json::json!(16_777_217_u64),
        serde_json::json!(1_152_921_504_606_846_977_u64),
    ] {
        let mut changed = raw.clone();
        changed["vector"][0] = value;
        assert!(typed("chunk_embedding", &changed.to_string()).is_err());
    }
    assert!(super::super::exact_f32_vector(&serde_json::json!(["0.0"])).is_err());
    let both = super::super::exact_f32_vector(&serde_json::json!([0.0, -0.0])).unwrap();
    assert_eq!(both[0].to_bits(), 0.0_f32.to_bits());
    assert_eq!(both[1].to_bits(), (-0.0_f32).to_bits());
}

#[test]
fn mandatory_hold_requirement_must_match_original_work_inventory() {
    let directory = tempfile::tempdir().unwrap();
    let plan = fixture(directory.path(), vec![], BTreeMap::new());
    let mut spool = SourceSpool::open(&plan, &directory.path().join("scratch")).unwrap();
    spool.historical_work_counts = BTreeMap::from([
        ("work_item".into(), 2),
        ("work_item_archive".into(), 1),
        ("running".into(), 1),
    ]);
    assert!(spool.validate_requirements().is_err());
    let requirement = serde_json::json!({"kind":"historical_work_hold_v1","snapshot_sha256":spool.source_sha256,"rows":spool.historical_work_counts,"original_payload_unchanged":true,"verified":false,"must_complete":"register and reconcile"});
    spool.runtime_requirements = vec![requirement.clone()];
    spool.validate_requirements().unwrap();
    for (key, value) in [
        ("snapshot_sha256", serde_json::json!("b".repeat(64))),
        (
            "rows",
            serde_json::json!({"work_item":1,"work_item_archive":1,"running":1}),
        ),
        ("verified", serde_json::json!(true)),
        ("kind", serde_json::json!("unknown")),
    ] {
        spool.runtime_requirements[0] = requirement.clone();
        spool.runtime_requirements[0][key] = value;
        assert!(spool.validate_requirements().is_err());
    }
    spool.runtime_requirements = vec![requirement.clone(), requirement];
    assert!(spool.validate_requirements().is_err());
}

fn media_plan(root: &Path, external: bool) -> PathBuf {
    use elephant_types::records::{DocMediaKind, MediaAsset};
    let media = MediaAsset {
        base: BaseFields::new(
            RecordId::new_v4(TableName::MediaAsset),
            "synthetic".into(),
            "original".into(),
        ),
        kind: DocMediaKind::Image,
        media_type: "image/png".into(),
        storage_path: "../../never-opened".into(),
        size_bytes: 3,
        sha256: hex::encode(Sha256::digest([1_u8, 2, 3])),
        payload_base64: if external { None } else { Some("AQID".into()) },
        duration_ms: None,
        label: None,
    };
    std::fs::write(root.join("media.json"), serde_json::to_vec(&media).unwrap()).unwrap();
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = std::process::Command::new("python3")
        .arg("-c").arg(r"
import sys,json
from pathlib import Path
sys.path.insert(0, str(Path(sys.argv[1]) / 'scripts/migration'))
import preservation_plan as p
root=Path(sys.argv[2]); snapshot=root/'snapshot.json'; ids=root/'ids.jsonl'
snapshot.write_bytes(p.canonical({'backend':'elephant-db','version':1,'exported_at':'2026-10-10T00:00:00Z','tables':{'media_asset':[json.loads((root/'media.json').read_text())]}}))
ids.write_bytes(b'')
p.inventory(snapshot,ids,root/'plan',space='synthetic',snapshot_sha256=p.file_sha(snapshot),idmap_sha256=p.file_sha(ids))
").arg(workspace).arg(root).env("PYTHONDONTWRITEBYTECODE","1").output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    root.join("plan/plan.json")
}

#[test]
fn python_inline_media_proof_is_bound_to_exact_spool_and_retained() {
    let dir = tempfile::tempdir().unwrap();
    let path = media_plan(dir.path(), false);
    let spool = SourceSpool::open(&path, &dir.path().join("checks.sqlite")).unwrap();
    assert_eq!(spool.media_preservation["decoded_inline_bytes"], 3);
    assert_eq!(spool.media_preservation["inline_rows"], 1);
    assert_eq!(spool.media_preservation["self_contained"], true);
    let rows = spool.page("media_asset", None).unwrap();
    let value = strict_json(&rows[0].lexical).unwrap();
    assert_eq!(value["payload_base64"], "AQID");
    assert_eq!(value["storage_path"], "../../never-opened");
    // A display-plan change cannot substitute for the inventory-bound proof.
    let mut plan: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    plan["media_preservation"]["decoded_inline_bytes"] = 4.into();
    std::fs::write(&path, serde_json::to_vec(&plan).unwrap()).unwrap();
    assert!(SourceSpool::open(&path, &dir.path().join("tampered.sqlite")).is_err());
    // Even a consistently rebound metadata row must agree with original rows.
    let db = Connection::open(path.parent().unwrap().join("inventory.sqlite")).unwrap();
    db.execute(
        "UPDATE metadata SET v=? WHERE k='media_preservation'",
        [plan["media_preservation"].to_string()],
    )
    .unwrap();
    db.close().unwrap();
    rebind(path.parent().unwrap());
    assert!(SourceSpool::open(&path, &dir.path().join("wrong-count.sqlite")).is_err());
}

#[test]
fn python_external_media_remains_refused_even_if_blocker_is_removed() {
    let dir = tempfile::tempdir().unwrap();
    let path = media_plan(dir.path(), true);
    assert!(SourceSpool::open(&path, &dir.path().join("blocked.sqlite")).is_err());
    let mut plan: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    plan["runtime_blockers"] = serde_json::json!([]);
    let db = Connection::open(path.parent().unwrap().join("inventory.sqlite")).unwrap();
    db.execute("UPDATE metadata SET v='[]' WHERE k='runtime_blockers'", [])
        .unwrap();
    db.close().unwrap();
    std::fs::write(&path, serde_json::to_vec(&plan).unwrap()).unwrap();
    rebind(path.parent().unwrap());
    assert!(SourceSpool::open(&path, &dir.path().join("still-blocked.sqlite")).is_err());
}

#[test]
fn continuation_page_uses_composite_key_seek() {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch("CREATE TABLE rows(t TEXT,k TEXT,h TEXT,space TEXT,PRIMARY KEY(t,k)); CREATE TABLE raw_rows(t TEXT,k TEXT,lexical BLOB,lexical_sha256 TEXT,PRIMARY KEY(t,k));").unwrap();
    let sql = format!("EXPLAIN QUERY PLAN {}", spool_page_sql(true));
    let mut statement = db.prepare(&sql).unwrap();
    let details = statement
        .query_map(params!["doc", "doc:9000", 256], |r| r.get::<_, String>(3))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert!(
        details
            .iter()
            .any(|d| d.contains("SEARCH r USING INDEX") && d.contains("t=? AND k>?")),
        "{details:?}"
    );
    assert!(
        !details
            .iter()
            .any(|d| d.contains("SCAN") || d.contains("TEMP B-TREE")),
        "{details:?}"
    );
}

#[test]
fn operational_disposition_requires_exact_spool_metadata_including_presence() {
    for (plan_mode, spool_mode, valid) in [
        (None, None, true),
        (Some("native"), Some("native"), true),
        (Some("managed_archive_v1"), Some("managed_archive_v1"), true),
        (Some("managed_archive_v1"), Some("native"), false),
        (None, Some("managed_archive_v1"), false),
        (Some("managed_archive_v1"), None, false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = fixture(
            dir.path(),
            vec![],
            BTreeMap::from([("work_edge".into(), 0)]),
        );
        let mut plan: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        if let Some(mode) = plan_mode {
            plan["operational_disposition"] = mode.into();
        }
        if plan_mode == Some("managed_archive_v1") {
            let mut hash = Sha256::new_with_prefix(b"recall-operational-archive-rows-v1\0");
            hash.update(serde_json::to_vec(&serde_json::json!(["work_edge", 0])).unwrap());
            let proof = serde_json::json!({"format":"recall-operational-archive-source-inventory-v1",
                "disposition_sha256":elephant_storage::historical_operational_archive::ArchiveBinding::disposition_digest(),
                "tables":{"work_edge":0},"rows":0,"lexical_bytes":0,
                "ordered_inventory_sha256":hex::encode(hash.finalize()),"runtime_verified":false});
            plan["operational_archive_inventory"] = proof.clone();
            plan["runtime_requirements"] = serde_json::json!([{"kind":"managed_operational_archive_v1",
                "snapshot_sha256":plan["snapshot_sha256"],"source_inventory":proof,
                "historical_work":{"work_item":0,"work_item_archive":0,"running":0},
                "disposition":"archive_inert","verified":false,"must_complete":"retained managed archive"}]);
            let db = Connection::open(dir.path().join("inventory.sqlite")).unwrap();
            for key in ["operational_archive_inventory", "runtime_requirements"] {
                db.execute(
                    "INSERT INTO metadata VALUES (?,?)",
                    params![key, plan[key].to_string()],
                )
                .unwrap();
            }
            db.close().unwrap();
        }
        if let Some(mode) = spool_mode {
            let db = Connection::open(dir.path().join("inventory.sqlite")).unwrap();
            db.execute(
                "INSERT INTO metadata VALUES ('operational_disposition',?)",
                [serde_json::to_string(mode).unwrap()],
            )
            .unwrap();
            db.close().unwrap();
        }
        plan["inventory_sha256"] = file_sha(&dir.path().join("inventory.sqlite"))
            .unwrap()
            .into();
        std::fs::write(&path, serde_json::to_vec(&plan).unwrap()).unwrap();
        let spool = SourceSpool::open(&path, &dir.path().join("checks.sqlite"));
        assert_eq!(spool.is_ok(), valid, "{plan_mode:?}/{spool_mode:?}");
        if let Ok(spool) = spool {
            assert_eq!(
                spool.archive_tables().contains_key("work_edge"),
                plan_mode == Some("managed_archive_v1")
            );
        }
    }
}
