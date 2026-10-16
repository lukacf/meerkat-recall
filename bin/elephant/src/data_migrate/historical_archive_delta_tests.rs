//! Public initial/delta adapter controls with native on-disk state and real planner output.
use super::*;
use elephant_storage::historical_archive_lifecycle::{
    current_operational_archive_receipt, with_operational_archive_read,
};
use elephant_storage::Repository as _;
fn python(root: &Path, code: &str, args: &[&str]) {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let result = std::process::Command::new("python3")
        .args(["-c", code])
        .arg(workspace)
        .arg(root)
        .args(args)
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}
async fn initial(root: &Path) -> StorageClient {
    super::super::super::streaming::integration_tests::planner_fixture(root);
    python(
        root,
        r"
import json,sys
from pathlib import Path
sys.path.insert(0,str(Path(sys.argv[1])/'scripts/migration'))
import preservation_plan as p
r=Path(sys.argv[2]);s=r/'snapshot.json';v=json.loads(s.read_text());v['tables']['system_event'][0]['seq']=100;s.write_bytes(p.canonical(v))
p.inventory(s,r/'idmap.jsonl',r/'archive-plan',space='default',target_space='household',snapshot_sha256=p.file_sha(s),idmap_sha256=p.file_sha(r/'idmap.jsonl'),operational_disposition='managed_archive_v1')
",
        &[],
    );
    let plan = root.join("archive-plan/plan.json");
    let config = super::super::super::streaming::integration_tests::write_config(root);
    let client = open(root).await;
    let ledger = root.join("initial.sqlite");
    let report = root.join("initial.json");
    let receipts = root.join("initial.adoption.jsonl");
    let spool = SourceSpool::open(&plan, &ledger).unwrap();
    let adoption = super::super::super::historical_adopt::prepare(
        &spool,
        &plan,
        &config,
        &root.join("initial-adoption.sqlite"),
    )
    .unwrap();
    let sha = spool.source_sha256.clone();
    client
        .with_transfer_barrier(&sha, move |exclusive| async move {
            super::super::super::streaming::copy(
                &spool, &exclusive, &ledger, &report, adoption, &receipts,
            )
            .await
            .map_err(storage_error)
        })
        .await
        .unwrap();
    client
}
fn cut(root: &Path, source: &str, baseline: &str, label: &str, remove: bool) {
    python(
        root,
        r"
import copy,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(sys.argv[1])/'scripts/migration'))
import preservation_plan as p
r=Path(sys.argv[2]);old=json.loads((r/sys.argv[3]).read_text());v=copy.deepcopy(old);label=sys.argv[5];v['exported_at']='2026-10-16T'+('01' if label=='s1' else '02')+':00:00Z';changes=[]
if sys.argv[6]=='yes':
 for row in v['tables']['system_event']:
  changes.append({'table':'system_event','key':row['id'],'class':'operational','kind':'removed','flags':[],'changed_fields':sorted(row),'dependency_rank':1,'updated_at':{'s0':row.get('updated_at'),'s1':None},'deleted_at':{'s0':row.get('deleted_at'),'s1':None},'row_sha256':{'s0':p.value_hash(row),'s1':None},'before':row,'after':None})
 v['tables']['system_event']=[]
s=r/(label+'.json');s.write_bytes(p.canonical(v));d=r/(label+'-changes.jsonl');d.write_bytes(b''.join(p.canonical(c)+b'\n' for c in changes));b=r/sys.argv[4];bp=json.loads((b/'plan.json').read_text())
p.inventory(s,r/'idmap.jsonl',r/(label+'-plan'),space='default',target_space='household',snapshot_sha256=p.file_sha(s),idmap_sha256=p.file_sha(r/'idmap.jsonl'),operational_disposition='managed_archive_v1')
p.delta(b/'inventory.sqlite',d,s,r/(label+'-delta'),before_snapshot_sha256=bp['snapshot_sha256'],after_snapshot_sha256=p.file_sha(s),changes_sha256=p.file_sha(d),baseline_sha256=bp['inventory_sha256'])
",
        &[source, baseline, label, if remove { "yes" } else { "no" }],
    );
}
fn command(
    root: &Path,
    attempt: &str,
    cut: &str,
    baseline_plan: &str,
    report: &str,
) -> DataMigrateDeltaArgs {
    let mut a = args(root, attempt);
    a.baseline_plan = root.join(baseline_plan).join("plan.json");
    a.baseline_report = root.join(report);
    a.baseline_receipts = a.baseline_report.with_extension("catchup-bindings.jsonl");
    a.baseline_ledger = if report == "initial.json" {
        root.join("initial.sqlite")
    } else {
        a.baseline_report.with_extension("catchup.sqlite")
    };
    a.delta_plan = root.join(format!("{cut}-delta/delta.json"));
    a.s1_plan = root.join(format!("{cut}-plan/plan.json"));
    a.accept_deletions = true;
    a
}
async fn assert_floor(client: &StorageClient, through: i64) {
    #[derive(Deserialize)]
    struct Cursor {
        consumer: String,
        seq: i64,
        retained: i64,
    }
    #[derive(Deserialize)]
    struct Count {
        n: u64,
    }
    let mut response=client.query_checked("SELECT consumer,seq,historical_baseline.through_seq AS retained FROM outbox_checkpoint WHERE space_id='household' AND consumer IN ['knowledge_views','truth_maintenance'] ORDER BY consumer").await.unwrap();
    let rows: Vec<Cursor> = response.take(0).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter().map(|r| r.consumer.as_str()).collect::<Vec<_>>(),
        vec!["knowledge_views", "truth_maintenance"]
    );
    for row in rows {
        assert_eq!(row.seq, through);
        assert_eq!(row.retained, through);
    }
    let mut response = client
        .query_checked("SELECT count() AS n FROM work_item GROUP ALL")
        .await
        .unwrap();
    let rows: Vec<Count> = response.take(0).unwrap();
    assert!(
        rows.is_empty() || rows[0].n == 0,
        "archived work cannot dispatch"
    );
}
#[test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain each original native client and authority repository through the complete archive recovery and exact journal assertions."
)]
fn command_archive_partial_payload_commit_ledger_gap_and_three_cut_tail_preservation() {
    let dir = tempfile::tempdir().unwrap();
    let run = |f: Box<dyn FnOnce(tokio::runtime::Runtime)>| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        f(runtime);
    };
    let root = dir.path().to_owned();
    let first = root.clone();
    run(Box::new(move |rt| {
        rt.block_on(async {
            let client = initial(&first).await;
            assert_floor(&client, 100).await;
            cut(&first, "snapshot.json", "archive-plan", "s1", true);
            let e = run_inner(
                command(&first, "partial", "s1", "archive-plan", "initial.json"),
                Some(client.clone()),
                DeltaFault::AfterArchiveCandidate,
            )
            .await
            .unwrap_err();
            assert!(e.to_string().contains("after candidate payload"), "{e:#}");
            assert!(!client.recovery_status().ready);
            // The exact scratch path is read from the public argument constructor.
            let scratch = command(&first, "partial", "s1", "archive-plan", "initial.json").scratch;
            let staged = readonly(&scratch.join("delta.sqlite")).unwrap();
            let columns: Vec<String> = staged
                .prepare("PRAGMA table_info(changes)")
                .unwrap()
                .query_map([], |r| r.get(1))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            assert!(!columns
                .iter()
                .any(|c| c == "before_raw" || c == "after_raw"));
        });
    }));
    let db_gap = root.clone();
    run(Box::new(move |rt| {
        rt.block_on(async {
            let client = open(&db_gap).await;
            let error = run_inner(
                command(&db_gap, "db-gap", "s1", "archive-plan", "initial.json"),
                Some(client.clone()),
                DeltaFault::AfterArchiveDatabaseCommit,
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains("before registry"), "{error:#}");
            assert!(!client.recovery_status().ready);
        });
    }));
    let second = root.clone();
    run(Box::new(move |rt| {
        rt.block_on(async {
            let client = open(&second).await;
            let e = run_inner(
                command(&second, "committed", "s1", "archive-plan", "initial.json"),
                Some(client.clone()),
                DeltaFault::AfterArchiveCommit,
            )
            .await
            .unwrap_err();
            assert!(e.to_string().contains("after archive DB"), "{e:#}");
            assert!(!client.recovery_status().ready);
        });
    }));
    run(Box::new(move |rt| {
        rt.block_on(async {
            let client = open(&root).await;
            run_inner(
                command(&root, "s1-complete", "s1", "archive-plan", "initial.json"),
                Some(client.clone()),
                DeltaFault::None,
            )
            .await
            .unwrap();
            assert_floor(&client, 100).await;
            let first = header(&root.join("s1-complete.json")).unwrap();
            assert_eq!(first["batches"], 0);
            assert_eq!(first["archived_changes"], 1);
            assert_eq!(first["source_tables"]["system_event"], 0);
            let archive = current_operational_archive_receipt(&client, "household")
                .await
                .unwrap()
                .unwrap();
            let rows = with_operational_archive_read(&client, &archive, |reader| {
                reader.page("system_event", None, 256)
            })
            .await
            .unwrap();
            assert!(rows.is_empty());
            cut(&root, "s1.json", "s1-plan", "s2", false);
            run_inner(
                command(&root, "s2-complete", "s2", "s1-plan", "s1-complete.json"),
                Some(client.clone()),
                DeltaFault::None,
            )
            .await
            .unwrap();
            assert_floor(&client, 100).await;
            assert!(client.recovery_status().ready);
        });
    }));
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain each original native client and authority repository through the complete archive recovery and exact journal assertions."
)]
async fn command_archive_same_epoch_unknown_cursor_field_refuses_without_publication() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let client = initial(root).await;
    cut(root, "snapshot.json", "archive-plan", "s1", true);
    let before = current_operational_archive_receipt(&client, "household")
        .await
        .unwrap()
        .unwrap();
    let canonical = elephant_storage::canonical_commit::CanonicalRepository::new(client.clone());
    let epoch = canonical.observe_epoch("household").await.unwrap();
    client.query_checked("UPDATE outbox_checkpoint SET unrecognized_archive_authority=true WHERE space_id='household' AND consumer='knowledge_views'").await.unwrap();
    assert_eq!(canonical.observe_epoch("household").await.unwrap(), epoch);
    let error = run_inner(
        command(root, "tampered", "s1", "archive-plan", "initial.json"),
        Some(client.clone()),
        DeltaFault::None,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("archive_catchup_before_image_changed"),
        "{error:#}"
    );
    assert!(!root.join("tampered.json").exists());
    assert!(!client.recovery_status().ready);
    // Failed custody must deny ordinary reads. Inspect retained payload-free
    // journal metadata instead; this grants no native/readiness bypass.
    assert!(current_operational_archive_receipt(&client, "household")
        .await
        .is_err());
    let journal = client.erasure_journal().unwrap().path();
    let index: serde_json::Value = serde_json::from_slice(
        &std::fs::read(journal.join("reference-membership-index.json")).unwrap(),
    )
    .unwrap();
    let selected = index["payload"]["operational_archive_registry_sha256"]
        .as_str()
        .unwrap();
    let registry: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            journal
                .join("operational-archive-registries")
                .join(format!("{selected}.json")),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        registry["payload"]["heads"][&before.registration.lineage_id],
        before.registration.generation_id
    );
    let original = &registry["payload"]["entries"][&before.registration.generation_id];
    assert_eq!(original["state"]["state"], "committed");
    assert_eq!(
        original["state"]["native_receipt_sha256"],
        before.native_receipt_sha256
    );
}

fn mixed_cut(root: &Path, source: &str, baseline: &str, label: &str, restore: bool) {
    python(
        root,
        r"
import copy,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(sys.argv[1])/'scripts/migration'))
import preservation_plan as p
r=Path(sys.argv[2]);old=json.loads((r/sys.argv[3]).read_text());v=copy.deepcopy(old);label=sys.argv[5];restore=sys.argv[6]=='yes';v['exported_at']='2026-10-16T'+('04' if restore else '03')+':00:00Z'
v['tables']['work_item'][0]['deleted_at']=None if restore else '2026-10-16T03:00:00Z'
v['tables']['work_item'][0]['payload']['archive_marker']='restored exact history' if restore else 'soft-deleted exact history'
v['tables']['entity'][0]['display_name']='mixed native restored' if restore else 'mixed native deleted'
changes=[]
for table in ['work_item','entity']:
 a=old['tables'][table][0];b=v['tables'][table][0]
 changes.append({'table':table,'key':a['id'],'class':'operational' if table=='work_item' else 'graph','kind':p.change_kind(a,b),'flags':[],'changed_fields':sorted(k for k in set(a)|set(b) if a.get(k)!=b.get(k)),'dependency_rank':1,'updated_at':{'s0':a.get('updated_at'),'s1':b.get('updated_at')},'deleted_at':{'s0':a.get('deleted_at'),'s1':b.get('deleted_at')},'row_sha256':{'s0':p.value_hash(a),'s1':p.value_hash(b)},'before':a,'after':b})
s=r/(label+'.json');s.write_bytes(p.canonical(v));d=r/(label+'-changes.jsonl');d.write_bytes(b''.join(p.canonical(c)+b'\n' for c in changes));base=r/sys.argv[4];bp=json.loads((base/'plan.json').read_text())
p.inventory(s,r/'idmap.jsonl',r/(label+'-plan'),space='default',target_space='household',snapshot_sha256=p.file_sha(s),idmap_sha256=p.file_sha(r/'idmap.jsonl'),operational_disposition='managed_archive_v1')
p.delta(base/'inventory.sqlite',d,s,r/(label+'-delta'),before_snapshot_sha256=bp['snapshot_sha256'],after_snapshot_sha256=p.file_sha(s),changes_sha256=p.file_sha(d),baseline_sha256=bp['inventory_sha256'])
",
        &[source, baseline, label, if restore { "yes" } else { "no" }],
    );
}
async fn assert_mixed_cut(client: &StorageClient, root: &Path, snapshot: &str, report: &str) {
    let source: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join(snapshot)).unwrap()).unwrap();
    let receipt = current_operational_archive_receipt(client, "household")
        .await
        .unwrap()
        .unwrap();
    let rows = with_operational_archive_read(client, &receipt, |reader| {
        reader.page("work_item", None, 256)
    })
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    let historical: serde_json::Value = serde_json::from_slice(&rows[0].lexical).unwrap();
    assert_eq!(
        historical, source["tables"]["work_item"][0],
        "original namespace, status, attempts, deletion and payload are preserved"
    );
    let entity: elephant_types::records::Entity =
        serde_json::from_value(source["tables"]["entity"][0].clone()).unwrap();
    let native = elephant_storage::EntityRepository::new(client.clone())
        .get("household", &entity.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(native.display_name, entity.display_name);
    let result = header(&root.join(report)).unwrap();
    assert_eq!(result["changes"], 2);
    assert_eq!(result["archived_changes"], 1);
    assert_eq!(result["batches"], 1);
    assert_eq!(result["archive_batches"], 1);
    assert_eq!(result["source_tables"]["work_item"], 1);
    assert_floor(client, 100).await;
}
#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain each original native client and authority repository through the complete archive recovery and exact journal assertions."
)]
async fn command_mixed_native_archive_soft_delete_requires_intent_then_restore_preserves_history() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let client = initial(root).await;
    mixed_cut(root, "snapshot.json", "archive-plan", "mixed1", false);
    let mut refused = command(
        root,
        "missing-intent",
        "mixed1",
        "archive-plan",
        "initial.json",
    );
    refused.accept_deletions = false;
    let error = run_inner(refused, Some(client.clone()), DeltaFault::None)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("accept-deletions"), "{error:#}");
    assert!(
        client.recovery_status().ready,
        "intent refusal precedes custody"
    );
    assert!(!root.join("missing-intent.json").exists());
    run_inner(
        command(
            root,
            "mixed1-done",
            "mixed1",
            "archive-plan",
            "initial.json",
        ),
        Some(client.clone()),
        DeltaFault::None,
    )
    .await
    .unwrap();
    assert_mixed_cut(&client, root, "mixed1.json", "mixed1-done.json").await;
    mixed_cut(root, "mixed1.json", "mixed1-plan", "mixed2", true);
    let mut restore = command(
        root,
        "mixed2-done",
        "mixed2",
        "mixed1-plan",
        "mixed1-done.json",
    );
    restore.accept_deletions = false;
    run_inner(restore, Some(client.clone()), DeltaFault::None)
        .await
        .unwrap();
    assert_mixed_cut(&client, root, "mixed2.json", "mixed2-done.json").await;
    assert!(client.recovery_status().ready);
}

#[tokio::test]
async fn command_archive_refuses_target_events_written_after_initial_cut() {
    use elephant_types::{
        records::{BaseFields, SystemEvent},
        RecordId, TableName,
    };
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let client = initial(root).await;
    let published = elephant_storage::event_sequence::append_system_event(
        &client,
        SystemEvent {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SystemEvent),
                "household".into(),
                "post-cut-producer".into(),
            ),
            seq: 0,
            event_type: "records.changed".into(),
            payload: serde_json::json!({"new_target_event":true}),
        },
    )
    .await
    .unwrap();
    assert_eq!(published.seq, 101);
    let original = current_operational_archive_receipt(&client, "household")
        .await
        .unwrap()
        .unwrap();
    cut(root, "snapshot.json", "archive-plan", "s1", true);
    let error = run_inner(
        command(root, "post-live", "s1", "archive-plan", "initial.json"),
        Some(client.clone()),
        DeltaFault::None,
    )
    .await
    .unwrap_err();
    // The native producer floor changed. Admission can reject at the current
    // epoch fence or the archive transaction's complete original before-image.
    let message = format!("{error:#}");
    assert!(
        message.contains("epoch changed")
            || message.contains("before_image_changed")
            || message.contains("live_events"),
        "{message}"
    );
    assert!(!root.join("post-live.json").exists());
    let journal = client.erasure_journal().unwrap().path();
    let index: serde_json::Value = serde_json::from_slice(
        &std::fs::read(journal.join("reference-membership-index.json")).unwrap(),
    )
    .unwrap();
    let selected = index["payload"]["operational_archive_registry_sha256"]
        .as_str()
        .unwrap();
    let registry: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            journal
                .join("operational-archive-registries")
                .join(format!("{selected}.json")),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        registry["payload"]["heads"][&original.registration.lineage_id],
        original.registration.generation_id
    );
    if client.recovery_status().ready {
        assert_eq!(
            elephant_storage::SystemEventRepository::new(client.clone())
                .get("household", &published.base.id)
                .await
                .unwrap(),
            Some(published)
        );
    } else {
        assert!(
            current_operational_archive_receipt(&client, "household")
                .await
                .is_err(),
            "pending custody still fences ordinary reads"
        );
    }
}
