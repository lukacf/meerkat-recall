//! Exact legacy assertion wrappers across original S0 and a complete S1 delta.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::super::streaming::legacy_assertion_literal_integration_tests as fixture;
use super::*;
use elephant_storage::{AssertionRepository, Repository, StorageClient, StorageConfig};
use elephant_types::RecordId;

fn successor(root: &Path) {
    fixture::python(
        root,
        r"
import copy,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(sys.argv[1])/'scripts/migration'))
import preservation_plan as p
r=Path(sys.argv[2]);old=json.loads((r/'literal-snapshot.json').read_text());new=copy.deepcopy(old);new['exported_at']='2026-10-16T00:00:00Z'
for row in new['tables']['assertion']:
 if row['assertion_key']=='string-live': row['object_literal']['value']='updated original';row['updated_at']='2026-10-16T00:00:00Z'
 if row['assertion_key']=='boolean-live': row['deleted_at']='2026-10-16T00:00:00Z';row['updated_at']=row['deleted_at']
for row in new['tables']['attribute']:
 if row['attr_type']=='legacy_string' and not row.get('deleted_at'): row['value']['value']='updated original';row['updated_at']='2026-10-16T00:00:00Z'
 if row['attr_type']=='legacy_boolean' and not row.get('deleted_at'): row['deleted_at']='2026-10-16T00:00:00Z';row['updated_at']=row['deleted_at']
snapshot=r/'literal-s1.json';snapshot.write_bytes(p.canonical(new));changes=[]
for table in sorted(old['tables']):
 before={x['id']:x for x in old['tables'][table]};after={x['id']:x for x in new['tables'][table]}
 for key in sorted(before.keys()|after.keys()):
  a,b=before.get(key),after.get(key)
  if a==b:continue
  changes.append({'table':table,'key':key,'class':'graph','kind':p.change_kind(a,b),'flags':[],'changed_fields':sorted(k for k in a.keys()|b.keys() if a.get(k)!=b.get(k)),'dependency_rank':1,'updated_at':{'s0':a.get('updated_at'),'s1':b.get('updated_at')},'deleted_at':{'s0':a.get('deleted_at'),'s1':b.get('deleted_at')},'row_sha256':{'s0':p.value_hash(a),'s1':p.value_hash(b)},'before':a,'after':b})
assert len(changes)==4
change_file=r/'literal-changes.jsonl';change_file.write_bytes(b''.join(p.canonical(c)+b'\n' for c in changes))
p.inventory(snapshot,r/'idmap.jsonl',r/'literal-s1-plan',space='default',target_space='household',snapshot_sha256=p.file_sha(snapshot),idmap_sha256=p.file_sha(r/'idmap.jsonl'),operational_disposition='managed_archive_v1')
plan=json.loads((r/'literal-plan/plan.json').read_text());p.delta(r/'literal-plan/inventory.sqlite',change_file,snapshot,r/'literal-delta',before_snapshot_sha256=plan['snapshot_sha256'],after_snapshot_sha256=p.file_sha(snapshot),changes_sha256=p.file_sha(change_file),baseline_sha256=plan['inventory_sha256'])
",
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original native client and repositories through exact S0/S1 custody, unchanged input checks and final owned close."
)]
async fn legacy_native_literals_s1_updates_and_deletes_keep_original_baseline_chain() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let client = StorageClient::new_with_durability(
        StorageConfig {
            url: format!("rocksdb://{}", root.join("target").display()),
            ..StorageConfig::default()
        },
        None,
        Some(root.join("journal")),
    )
    .await
    .unwrap();
    Box::pin(fixture::initial(root, &client)).await;
    let original_report = file_sha(&root.join("initial.json")).unwrap();
    let original_ledger = file_sha(&root.join("initial.sqlite")).unwrap();
    let original_bindings = file_sha(&root.join("initial.catchup-bindings.jsonl")).unwrap();
    let original_snapshot = file_sha(&root.join("literal-snapshot.json")).unwrap();
    successor(root);
    let args = DataMigrateDeltaArgs {
        baseline_plan: root.join("literal-plan/plan.json"),
        baseline_report: root.join("initial.json"),
        baseline_receipts: root.join("initial.catchup-bindings.jsonl"),
        baseline_ledger: root.join("initial.sqlite"),
        delta_plan: root.join("literal-delta/delta.json"),
        s1_plan: root.join("literal-s1-plan/plan.json"),
        historical_config: root.join("space-request.json"),
        scratch: root.join("s1-scratch"),
        report: root.join("s1-report.json"),
        accept_deletions: true,
        target: super::super::TransferTarget {
            url: Some(format!("rocksdb://{}", root.join("target").display())),
            namespace: None,
            database: None,
        },
    };
    Box::pin(run_inner(args, Some(client.clone()), DeltaFault::None))
        .await
        .unwrap();
    assert!(client.recovery_status().ready);
    let output: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("s1-report.json")).unwrap()).unwrap();
    assert_eq!(
        output["state"],
        "history_reconciled_current_embedding_coverage_pending"
    );
    assert_eq!(output["changes"], 4);
    assert_eq!(output["original_report_sha256"], original_report);
    assert_eq!(
        output["after_snapshot_sha256"],
        file_sha(&root.join("literal-s1.json")).unwrap()
    );
    assert_eq!(
        file_sha(&root.join("initial.json")).unwrap(),
        original_report
    );
    assert_eq!(
        file_sha(&root.join("initial.sqlite")).unwrap(),
        original_ledger
    );
    assert_eq!(
        file_sha(&root.join("initial.catchup-bindings.jsonl")).unwrap(),
        original_bindings
    );
    assert_eq!(
        file_sha(&root.join("literal-snapshot.json")).unwrap(),
        original_snapshot
    );
    let snapshot: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("literal-s1.json")).unwrap()).unwrap();
    let repo = AssertionRepository::new(client.clone());
    for raw in snapshot["tables"]["assertion"].as_array().unwrap() {
        let id: RecordId = raw["id"].as_str().unwrap().parse().unwrap();
        let actual = repo
            .get_with_deleted("household", &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            actual.object_literal.as_ref(),
            Some(&raw["object_literal"]["value"])
        );
        assert_eq!(
            actual.base.deleted_at.is_some(),
            raw.get("deleted_at").is_some()
        );
        if raw.get("deleted_at").is_some() {
            assert!(repo.get("household", &id).await.unwrap().is_none());
        }
    }
    drop(repo);
    client.close_owned_command_connection(60_000).await.unwrap();
}
