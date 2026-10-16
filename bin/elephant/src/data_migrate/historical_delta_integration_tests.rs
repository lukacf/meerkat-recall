//! The command runner, real Python plans, native initial transfer and durable resume.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use elephant_storage::{StorageClient, StorageConfig};

async fn open(root: &Path) -> StorageClient {
    StorageClient::new_with_durability(
        StorageConfig {
            url: format!("rocksdb://{}", root.join("target").display()),
            ..StorageConfig::default()
        },
        None,
        Some(root.join("journal")),
    )
    .await
    .unwrap()
}
fn args(root: &Path, attempt: &str) -> DataMigrateDeltaArgs {
    DataMigrateDeltaArgs {
        baseline_plan: root.join("plan/plan.json"),
        baseline_report: root.join("initial.json"),
        baseline_receipts: root.join("initial.catchup-bindings.jsonl"),
        baseline_ledger: root.join("initial.sqlite"),
        delta_plan: root.join("delta/delta.json"),
        s1_plan: root.join("s1-plan/plan.json"),
        historical_config: root.join("space-request.json"),
        scratch: root.join(attempt),
        report: root.join(format!("{attempt}.json")),
        accept_deletions: false,
        target: super::super::TransferTarget {
            url: Some(format!("rocksdb://{}", root.join("target").display())),
            namespace: None,
            database: None,
        },
    }
}
fn make_delta(root: &Path) {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let code = r"
import copy,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(sys.argv[1])/'scripts/migration'))
import preservation_plan as p
root=Path(sys.argv[2]); original=json.loads((root/'snapshot.json').read_text()); s1=copy.deepcopy(original)
before=copy.deepcopy(s1['tables']['work_item'][0]); after=s1['tables']['work_item'][0]
after['payload']['after_original_cut']=True
snapshot=root/'s1.json'; snapshot.write_bytes(p.canonical(s1))
change={'table':'work_item','key':before['id'],'class':'operational','kind':p.change_kind(before,after),'flags':[],
'changed_fields':['payload'],'dependency_rank':1,'updated_at':{'s0':before.get('updated_at'),'s1':after.get('updated_at')},
'deleted_at':{'s0':before.get('deleted_at'),'s1':after.get('deleted_at')},'row_sha256':{'s0':p.value_hash(before),'s1':p.value_hash(after)},'before':before,'after':after}
graph_before=copy.deepcopy(original['tables']['entity'][0]); graph_after=s1['tables']['entity'][0]
graph_after['display_name']='changed source-less graph'
snapshot.write_bytes(p.canonical(s1))
# Complete S1 inventory includes the graph-only edit.
p.inventory(snapshot,root/'idmap.jsonl',root/'s1-plan',space='default',target_space='household',snapshot_sha256=p.file_sha(snapshot),idmap_sha256=p.file_sha(root/'idmap.jsonl'))
graph_change={'table':'entity','key':graph_before['id'],'class':'graph','kind':'updated','flags':[],
'changed_fields':['display_name'],'dependency_rank':1,'updated_at':{'s0':graph_before.get('updated_at'),'s1':graph_after.get('updated_at')},
'deleted_at':{'s0':graph_before.get('deleted_at'),'s1':graph_after.get('deleted_at')},'row_sha256':{'s0':p.value_hash(graph_before),'s1':p.value_hash(graph_after)},'before':graph_before,'after':graph_after}
diff=root/'requested-delta.jsonl'; diff.write_bytes(p.canonical(change)+b'\n'+p.canonical(graph_change)+b'\n')
plan=json.loads((root/'plan/plan.json').read_text())
p.delta(root/'plan/inventory.sqlite',diff,snapshot,root/'delta',before_snapshot_sha256=plan['snapshot_sha256'],after_snapshot_sha256=p.file_sha(snapshot),changes_sha256=p.file_sha(diff),baseline_sha256=plan['inventory_sha256'])
";
    let output = std::process::Command::new("python3")
        .args(["-c", code])
        .arg(workspace)
        .arg(root)
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
pub async fn initial_population_fixture(root: &Path) -> StorageClient {
    let plan = super::super::streaming::integration_tests::planner_fixture(root);
    let config = super::super::streaming::integration_tests::write_config(root);
    let client = open(root).await;
    let ledger = root.join("initial.sqlite");
    let report = root.join("initial.json");
    let receipts = root.join("initial.adoption.jsonl");
    let spool = SourceSpool::open(&plan, &ledger).unwrap();
    let adoption = super::super::historical_adopt::prepare(
        &spool,
        &plan,
        &config,
        &root.join("initial-adoption.sqlite"),
    )
    .unwrap();
    let sha = spool.source_sha256.clone();
    client
        .with_transfer_barrier(&sha, move |exclusive| async move {
            super::super::streaming::copy(&spool, &exclusive, &ledger, &report, adoption, &receipts)
                .await
                .map_err(storage_error)
        })
        .await
        .unwrap();
    client
}
#[expect(
    clippy::significant_drop_tightening,
    reason = "The original fixture client and derived repository handles retain native custody across this complete interruption/recovery scenario, including final receipt, epoch, byte and readiness assertions."
)]
async fn initial_and_interrupted(root: &Path) {
    let client = initial_population_fixture(root).await;
    make_delta(root);
    let interrupted = run_inner(
        args(root, "attempt-1"),
        Some(client.clone()),
        DeltaFault::AfterDatabaseCommit,
    )
    .await
    .expect_err("the selected post-commit interruption must be reached");
    assert_eq!(
        interrupted.to_string(),
        "Data integrity error: historical delta adapter: injected process interruption after DB commit before ledger",
        "unexpected pre-custody or pre-commit error: {interrupted:#}"
    );
    assert!(!client.recovery_status().ready);
    assert!(!root.join("attempt-1.json").exists());
    assert!(root
        .join("journal/historical-catchup-pending.json")
        .exists());
}
#[expect(
    clippy::significant_drop_tightening,
    reason = "The original fixture client and derived repository handles retain native custody across this complete interruption/recovery scenario, including final receipt, epoch, byte and readiness assertions."
)]
async fn resumed_before_report(root: &Path) {
    let client = open(root).await;
    assert!(!client.recovery_status().ready);
    let baseline = file_sha(&root.join("initial.sqlite")).unwrap();
    let error = run_inner(
        args(root, "attempt-2"),
        Some(client.clone()),
        DeltaFault::AfterRuntimeCompletion,
    )
    .await
    .expect_err("injected report gap");
    assert!(
        error
            .to_string()
            .contains("after runtime completion before report publication"),
        "unexpected earlier refusal: {error:#}"
    );
    assert!(!root.join("attempt-2.json").exists());
    assert!(client.recovery_status().ready);
    assert!(!root
        .join("journal/historical-catchup-pending.json")
        .exists());
    assert_eq!(file_sha(&root.join("initial.sqlite")).unwrap(), baseline);
}
#[expect(
    clippy::significant_drop_tightening,
    reason = "The original fixture client and derived repository handles retain native custody across this complete interruption/recovery scenario, including final receipt, epoch, byte and readiness assertions."
)]
async fn report_recovered_then_second_delta(root: &Path) {
    use elephant_storage::Repository as _;
    #[derive(Deserialize)]
    struct Work {
        payload: serde_json::Value,
    }
    let client = open(root).await;
    assert!(client.recovery_status().ready);
    let canonical = elephant_storage::canonical_commit::CanonicalRepository::new(client.clone());
    let before_epoch = canonical.observe_epoch("household").await.unwrap();
    // A corrupt completed artifact fails closed without poisoning an initially
    // ready destination. A matching pending marker must remain unready.
    let completed_dir = root.join("journal/historical-catchup-completed");
    let completed = std::fs::read_dir(&completed_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .ends_with(".completed.json")
        })
        .unwrap();
    let original_completion = std::fs::read(&completed).unwrap();
    let receipt: serde_json::Value = serde_json::from_slice(&original_completion).unwrap();
    std::fs::write(&completed, b"corrupt completion").unwrap();
    assert!(run_inner(
        args(root, "corrupt-ready"),
        Some(client.clone()),
        DeltaFault::None
    )
    .await
    .is_err());
    assert!(
        client.recovery_status().ready,
        "pre-mutation completed retry refusal preserves Ready"
    );
    std::fs::write(&completed, &original_completion).unwrap();
    let pending = root.join("journal/historical-catchup-pending.json");
    std::fs::write(&pending, serde_json::to_vec(&receipt["marker"]).unwrap()).unwrap();
    std::fs::write(&completed, b"corrupt pending completion").unwrap();
    assert!(run_inner(
        args(root, "corrupt-pending"),
        Some(client.clone()),
        DeltaFault::None
    )
    .await
    .is_err());
    assert!(
        !client.recovery_status().ready,
        "pending failure must remain unavailable"
    );
    assert!(pending.exists());
    std::fs::write(&completed, &original_completion).unwrap();

    run_inner(
        args(root, "attempt-3"),
        Some(client.clone()),
        DeltaFault::None,
    )
    .await
    .unwrap();
    assert_eq!(
        canonical.observe_epoch("household").await.unwrap(),
        before_epoch,
        "completed retry must not mutate destination"
    );
    let output: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("attempt-3.json")).unwrap()).unwrap();
    assert_eq!(
        output["state"],
        "history_reconciled_current_embedding_coverage_pending"
    );
    assert_eq!(output["semantic_index_certified"], false);
    assert_eq!(output["changes"], 2);
    assert_eq!(output["catchup_baseline"]["bindings_count"], 3);
    let query = client
        .query_checked("SELECT payload FROM work_item WHERE space_id='household'")
        .await
        .unwrap();
    let mut query = query;

    let work: Vec<Work> = query.take(0).unwrap();
    assert_eq!(work.len(), 1);
    assert_eq!(work[0].payload["after_original_cut"], true);
    let original: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("s1.json")).unwrap()).unwrap();
    let entity_id: elephant_types::RecordId = original["tables"]["entity"][0]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let entity = elephant_storage::EntityRepository::new(client.clone())
        .get("household", &entity_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entity.display_name, "changed source-less graph");
    assert_eq!(entity.metadata, original["tables"]["entity"][0]["metadata"]);
    let retained = std::fs::read(root.join("attempt-3.json")).unwrap();
    let baseline_sha = file_sha(&root.join("attempt-3.catchup.sqlite")).unwrap();
    make_second_delta(root);
    let mut noop = args(root, "attempt-noop");
    noop.baseline_plan = root.join("s1-plan/plan.json");
    noop.baseline_report = root.join("attempt-3.json");
    noop.baseline_ledger = root.join("attempt-3.catchup.sqlite");
    noop.baseline_receipts = root.join("attempt-3.catchup-bindings.jsonl");
    noop.delta_plan = root.join("noop/delta.json");
    noop.s1_plan = root.join("s1-plan/plan.json");
    run_inner(noop, Some(client.clone()), DeltaFault::None)
        .await
        .unwrap();
    assert_eq!(
        canonical.observe_epoch("household").await.unwrap(),
        before_epoch
    );
    let noop_report = header(&root.join("attempt-noop.json")).unwrap();
    assert_eq!(noop_report["state"], "verified_unchanged_history");
    assert_eq!(noop_report["catchup_baseline"], output["catchup_baseline"]);
    let mut second = args(root, "attempt-4");
    second.baseline_plan = root.join("s1-plan/plan.json");
    second.baseline_report = root.join("attempt-3.json");
    second.baseline_ledger = root.join("attempt-3.catchup.sqlite");
    second.baseline_receipts = root.join("attempt-3.catchup-bindings.jsonl");
    second.delta_plan = root.join("delta2/delta.json");
    second.s1_plan = root.join("s2-plan/plan.json");
    run_inner(second, Some(client.clone()), DeltaFault::None)
        .await
        .unwrap();
    assert!(client.recovery_status().ready);
    assert_ne!(
        canonical.observe_epoch("household").await.unwrap(),
        before_epoch
    );
    assert_eq!(
        std::fs::read(root.join("attempt-3.json")).unwrap(),
        retained
    );
    assert_eq!(
        file_sha(&root.join("attempt-3.catchup.sqlite")).unwrap(),
        baseline_sha
    );
    let second = header(&root.join("attempt-4.json")).unwrap();
    assert_eq!(
        second["before_snapshot_sha256"],
        output["after_snapshot_sha256"]
    );
    assert_eq!(
        second["original_report_sha256"],
        file_sha(&root.join("attempt-3.json")).unwrap()
    );
    assert_ne!(
        second["catchup_baseline"]["expected_epoch"],
        output["catchup_baseline"]["expected_epoch"]
    );
}
#[test]
fn command_initial_transfer_delta_commit_gap_and_native_restart_reconcile() {
    let dir = tempfile::tempdir().unwrap();
    let first = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    first.block_on(initial_and_interrupted(dir.path()));
    drop(first); // Router and native store close before the second process runtime.
    let second = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    second.block_on(resumed_before_report(dir.path()));
    drop(second);
    let third = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    third.block_on(report_recovered_then_second_delta(dir.path()));
}

fn make_second_delta(root: &Path) {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let code = r"
import copy,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(sys.argv[1])/'scripts/migration'))
import preservation_plan as p
root=Path(sys.argv[2]); before_snapshot=json.loads((root/'s1.json').read_text()); after_snapshot=copy.deepcopy(before_snapshot)
before=copy.deepcopy(after_snapshot['tables']['work_item'][0]); after=after_snapshot['tables']['work_item'][0]
after['payload']['second_catchup']=True
snapshot=root/'s2.json'; snapshot.write_bytes(p.canonical(after_snapshot))
p.inventory(snapshot,root/'idmap.jsonl',root/'s2-plan',space='default',target_space='household',snapshot_sha256=p.file_sha(snapshot),idmap_sha256=p.file_sha(root/'idmap.jsonl'))
change={'table':'work_item','key':before['id'],'class':'operational','kind':p.change_kind(before,after),'flags':[], 'changed_fields':['payload'],'dependency_rank':1,'updated_at':{'s0':before.get('updated_at'),'s1':after.get('updated_at')},'deleted_at':{'s0':before.get('deleted_at'),'s1':after.get('deleted_at')},'row_sha256':{'s0':p.value_hash(before),'s1':p.value_hash(after)},'before':before,'after':after}
diff=root/'requested-delta2.jsonl'; diff.write_bytes(p.canonical(change)+b'\n')
plan=json.loads((root/'s1-plan/plan.json').read_text())
p.delta(root/'s1-plan/inventory.sqlite',diff,snapshot,root/'delta2',before_snapshot_sha256=plan['snapshot_sha256'],after_snapshot_sha256=p.file_sha(snapshot),changes_sha256=p.file_sha(diff),baseline_sha256=plan['inventory_sha256'])
empty=root/'noop.jsonl'; empty.write_bytes(b'')
p.delta(root/'s1-plan/inventory.sqlite',empty,root/'s1.json',root/'noop',before_snapshot_sha256=plan['snapshot_sha256'],after_snapshot_sha256=plan['snapshot_sha256'],changes_sha256=p.file_sha(empty),baseline_sha256=plan['inventory_sha256'])
";
    let output = std::process::Command::new("python3")
        .args(["-c", code])
        .arg(workspace)
        .arg(root)
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The empty predecessor is a storage-produced certificate, not an arbitrary
/// empty binding list. Exercise both commands and retain the original baseline.
pub async fn deleted_population_fixture(root: &Path) -> StorageClient {
    super::super::streaming::integration_tests::planner_fixture(root);
    make_empty_successor_plans(root);
    let config = super::super::streaming::integration_tests::write_config(root);
    let initial_plan = root.join("single-plan/plan.json");
    let ledger = root.join("initial.sqlite");
    let report = root.join("initial.json");
    let receipts = root.join("initial.adoption.jsonl");
    let spool = SourceSpool::open(&initial_plan, &ledger).unwrap();
    let adoption = super::super::historical_adopt::prepare(
        &spool,
        &initial_plan,
        &config,
        &root.join("initial-adoption.sqlite"),
    )
    .unwrap();
    let client = open(root).await;
    let digest = spool.source_sha256.clone();
    client
        .with_transfer_barrier(&digest, move |exclusive| async move {
            super::super::streaming::copy(&spool, &exclusive, &ledger, &report, adoption, &receipts)
                .await
                .map_err(storage_error)
        })
        .await
        .unwrap();
    let original = file_sha(&root.join("initial.sqlite")).unwrap();
    let empty_plan = root.join("empty-plan/plan.json");
    let empty_spool = SourceSpool::open(&empty_plan, &root.join("empty-check.sqlite")).unwrap();
    assert!(
        super::super::historical_adopt::prepare(
            &empty_spool,
            &empty_plan,
            &config,
            &root.join("initial-empty-refused.sqlite"),
        )
        .is_err(),
        "initial empty adoption remains refused"
    );
    let mut remove = args(root, "delete-without-intent");
    remove.baseline_plan = initial_plan.clone();
    remove.delta_plan = root.join("remove-delta/delta.json");
    remove.s1_plan = empty_plan.clone();
    let error = run_inner(remove, Some(client.clone()), DeltaFault::None)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("deletion"), "{error:#}");
    assert!(client.recovery_status().ready);
    let mut remove = args(root, "delete-last");
    remove.baseline_plan = initial_plan;
    remove.delta_plan = root.join("remove-delta/delta.json");
    remove.s1_plan = empty_plan.clone();
    remove.accept_deletions = true;
    run_inner(remove, Some(client.clone()), DeltaFault::None)
        .await
        .unwrap();
    let empty_report = header(&root.join("delete-last.json")).unwrap();
    assert_eq!(empty_report["catchup_baseline"]["bindings_count"], 0);
    assert_eq!(
        std::fs::metadata(root.join("delete-last.catchup-bindings.jsonl"))
            .unwrap()
            .len(),
        0
    );
    assert!(empty_report["catchup_baseline"]["receipt_source_id"].is_string());
    assert_eq!(file_sha(&root.join("initial.sqlite")).unwrap(), original);
    client
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "The original fixture client and derived repository handles retain native custody across this complete interruption/recovery scenario, including final receipt, epoch, byte and readiness assertions."
)]
async fn command_delete_last_group_then_create_from_completed_empty_baseline() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let client = Box::pin(deleted_population_fixture(root)).await;
    let empty_plan = root.join("empty-plan/plan.json");
    let empty_report = header(&root.join("delete-last.json")).unwrap();
    let mut create = args(root, "create-after-empty");
    create.baseline_plan = empty_plan;
    create.baseline_report = root.join("delete-last.json");
    create.baseline_ledger = root.join("delete-last.catchup.sqlite");
    create.baseline_receipts = root.join("delete-last.catchup-bindings.jsonl");
    create.delta_plan = root.join("create-delta/delta.json");
    create.s1_plan = root.join("created-plan/plan.json");
    run_inner(create, Some(client.clone()), DeltaFault::None)
        .await
        .unwrap();
    let created = header(&root.join("create-after-empty.json")).unwrap();
    assert_eq!(created["catchup_baseline"]["bindings_count"], 1);
    assert_eq!(
        created["before_snapshot_sha256"],
        empty_report["after_snapshot_sha256"]
    );
    assert!(client.recovery_status().ready);
}

fn make_empty_successor_plans(root: &Path) {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let code = r"
import copy,json,sys,uuid
from pathlib import Path
sys.path.insert(0,str(Path(sys.argv[1])/'scripts/migration'))
import preservation_plan as p
root=Path(sys.argv[2]); original=json.loads((root/'snapshot.json').read_text())
doc=next(d for d in original['tables']['doc'] if d['space_id']=='default')
rev=next(r for r in original['tables']['doc_revision'] if r['doc']==doc['id'])
nodes=[n for n in original['tables']['doc_node'] if n['revision']==rev['id']]
single={**original,'tables':{'doc':[doc],'doc_revision':[rev],'doc_node':nodes}}
empty={**original,'tables':{'doc':[],'doc_revision':[],'doc_node':[]}}
ids={r['id']:r['id'].split(':',1)[0]+':'+str(uuid.uuid5(uuid.NAMESPACE_URL,'new-after-empty:'+r['id'])) for rows in single['tables'].values() for r in rows}
def remap(value):
    if isinstance(value,str):return ids.get(value,value)
    if isinstance(value,list):return [remap(v) for v in value]
    if isinstance(value,dict):return {k:remap(v) for k,v in value.items()}
    return value
created=remap(copy.deepcopy(single))
for name,snapshot in [('single',single),('empty',empty),('created',created)]:
    path=root/(name+'.json');path.write_bytes(p.canonical(snapshot))
    idmap=root/(name+'-idmap.jsonl');idmap.write_bytes(b''.join(p.canonical({'old_doc_id':d['id'],'class':'source_less','content_sha256':p.value_hash({'title':d['title'],'text':d['text']})})+b'\n' for d in snapshot['tables']['doc']))
    p.inventory(path,idmap,root/(name+'-plan'),space='default',target_space='household',snapshot_sha256=p.file_sha(path),idmap_sha256=p.file_sha(idmap))
for label,before_name,after_name,before,after in [('remove','single','empty',single,empty),('create','empty','created',empty,created)]:
    changes=[]
    for table in sorted(before['tables']):
        old={r['id']:r for r in before['tables'][table]};new={r['id']:r for r in after['tables'][table]}
        for key in sorted(old.keys()|new.keys()):
            a=old.get(key);b=new.get(key)
            changes.append({'table':table,'key':key,'class':'document','kind':p.change_kind(a,b),'flags':[], 'changed_fields':sorted(set(a or {})|set(b or {})),'dependency_rank':1,'updated_at':{'s0':(a or {}).get('updated_at'),'s1':(b or {}).get('updated_at')},'deleted_at':{'s0':(a or {}).get('deleted_at'),'s1':(b or {}).get('deleted_at')},'row_sha256':{'s0':None if a is None else p.value_hash(a),'s1':None if b is None else p.value_hash(b)},'before':a,'after':b})
    diff=root/(label+'-changes.jsonl');diff.write_bytes(b''.join(p.canonical(c)+b'\n' for c in changes))
    plan=json.loads((root/(before_name+'-plan')/'plan.json').read_text());target=root/(after_name+'.json')
    p.delta(root/(before_name+'-plan')/'inventory.sqlite',diff,target,root/(label+'-delta'),before_snapshot_sha256=plan['snapshot_sha256'],after_snapshot_sha256=p.file_sha(target),changes_sha256=p.file_sha(diff),baseline_sha256=plan['inventory_sha256'])
";
    let output = std::process::Command::new("python3")
        .args(["-c", code])
        .arg(workspace)
        .arg(root)
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

// A new exporter timestamp changes the immutable source identity without
// changing any original row. The real planner must emit zero changes/batches.
fn make_fresh_zero_cut(root: &Path, source: &str, baseline: &str, idmap: &str, label: &str) {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let code = r"
import json,sys
from pathlib import Path
sys.path.insert(0,str(Path(sys.argv[1])/'scripts/migration'))
import preservation_plan as p
root=Path(sys.argv[2]); source=root/sys.argv[3]; baseline=root/sys.argv[4]; idmap=root/sys.argv[5]; label=sys.argv[6]
original=source.read_bytes(); snapshot=json.loads(original); snapshot['exported_at']='2026-10-15T01:00:00Z'
fresh=root/(label+'.json'); fresh.write_bytes(p.canonical(snapshot))
assert source.read_bytes()==original and p.file_sha(source)!=p.file_sha(fresh)
assert json.loads(original)['tables']==snapshot['tables']
p.inventory(fresh,idmap,root/(label+'-plan'),space='default',target_space='household',snapshot_sha256=p.file_sha(fresh),idmap_sha256=p.file_sha(idmap))
empty=root/(label+'-changes.jsonl');empty.write_bytes(b'')
plan=json.loads((baseline/'plan.json').read_text())
p.delta(baseline/'inventory.sqlite',empty,fresh,root/(label+'-delta'),before_snapshot_sha256=plan['snapshot_sha256'],after_snapshot_sha256=p.file_sha(fresh),changes_sha256=p.file_sha(empty),baseline_sha256=plan['inventory_sha256'])
current=json.loads((root/(label+'-plan')/'plan.json').read_text())
p.delta(root/(label+'-plan')/'inventory.sqlite',empty,fresh,root/(label+'-noop'),before_snapshot_sha256=current['snapshot_sha256'],after_snapshot_sha256=current['snapshot_sha256'],changes_sha256=p.file_sha(empty),baseline_sha256=current['inventory_sha256'])
";
    let output = std::process::Command::new("python3")
        .args(["-c", code])
        .arg(workspace)
        .arg(root)
        .args([source, baseline, idmap, label])
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn fresh_args(root: &Path, attempt: &str) -> DataMigrateDeltaArgs {
    let mut command = args(root, attempt);
    command.delta_plan = root.join("fresh-delta/delta.json");
    command.s1_plan = root.join("fresh-plan/plan.json");
    command
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "The original fixture client and derived repository handles retain native custody across this complete interruption/recovery scenario, including final receipt, epoch, byte and readiness assertions."
)]
async fn command_fresh_zero_row_cut_regenerates_and_recovers_report() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let client = initial_population_fixture(root).await;
    let source_sha = file_sha(&root.join("snapshot.json")).unwrap();
    let ledger_sha = file_sha(&root.join("initial.sqlite")).unwrap();
    let initial = header(&root.join("initial.json")).unwrap();
    make_fresh_zero_cut(root, "snapshot.json", "plan", "idmap.jsonl", "fresh");
    let error = run_inner(
        fresh_args(root, "fresh-prefix-gap"),
        Some(client.clone()),
        DeltaFault::AfterFirstRegenerationCommit,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("after first regeneration commit before ledger"),
        "{error:#}"
    );
    assert!(!client.recovery_status().ready);
    assert!(root
        .join("journal/historical-catchup-pending.json")
        .exists());
    assert!(!root.join("fresh-prefix-gap.json").exists());
    let error = run_inner(
        fresh_args(root, "fresh-report-gap"),
        Some(client.clone()),
        DeltaFault::AfterRuntimeCompletion,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("after runtime completion before report publication"),
        "{error:#}"
    );
    assert!(client.recovery_status().ready);
    assert!(!root.join("fresh-report-gap.json").exists());
    assert!(!root
        .join("journal/historical-catchup-pending.json")
        .exists());
    let canonical = elephant_storage::canonical_commit::CanonicalRepository::new(client.clone());
    let completed_epoch = canonical.observe_epoch("household").await.unwrap();
    run_inner(
        fresh_args(root, "fresh-complete"),
        Some(client.clone()),
        DeltaFault::None,
    )
    .await
    .unwrap();
    assert_eq!(
        canonical.observe_epoch("household").await.unwrap(),
        completed_epoch
    );
    let report = header(&root.join("fresh-complete.json")).unwrap();
    assert_eq!(report["changes"], 0);
    assert_eq!(report["batches"], 0);
    assert_eq!(report["regeneration_steps"], 3);
    assert_eq!(report["catchup_baseline"]["bindings_count"], 3);
    assert_eq!(
        report["source_sha256"],
        file_sha(&root.join("fresh.json")).unwrap()
    );
    assert_ne!(report["source_sha256"], source_sha);
    assert_ne!(
        report["catchup_baseline"]["bindings_sha256"],
        initial["catchup_baseline"]["bindings_sha256"]
    );
    assert_ne!(
        report["catchup_baseline"]["expected_epoch"],
        initial["catchup_baseline"]["expected_epoch"]
    );
    assert_eq!(
        report["state"],
        "history_reconciled_current_embedding_coverage_pending"
    );
    assert_eq!(file_sha(&root.join("snapshot.json")).unwrap(), source_sha);
    assert_eq!(file_sha(&root.join("initial.sqlite")).unwrap(), ledger_sha);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "The original fixture client and derived repository handles retain native custody across this complete interruption/recovery scenario, including final receipt, epoch, byte and readiness assertions."
)]
async fn command_fresh_zero_row_cut_refuses_extra_target_row_at_inventory_preflight() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let client = initial_population_fixture(root).await;
    make_fresh_zero_cut(root, "snapshot.json", "plan", "idmap.jsonl", "fresh");
    let canonical = elephant_storage::canonical_commit::CanonicalRepository::new(client.clone());
    let epoch = canonical.observe_epoch("household").await.unwrap();
    client
        .query_checked("CREATE zero_cut_unexpected:row CONTENT {scope:'synthetic'}")
        .await
        .unwrap();
    assert_eq!(
        canonical.observe_epoch("household").await.unwrap(),
        epoch,
        "fixture must reach the full inventory guard"
    );
    let error = run_inner(
        fresh_args(root, "fresh-refused"),
        Some(client.clone()),
        DeltaFault::None,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("historical catch-up inventory differs"),
        "{error:#}"
    );
    assert!(!root.join("fresh-refused.json").exists());
    assert!(root
        .join("journal/historical-catchup-pending.json")
        .exists());
    assert!(!client.recovery_status().ready);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "The original fixture client and derived repository handles retain native custody across this complete interruption/recovery scenario, including final receipt, epoch, byte and readiness assertions."
)]
async fn command_certified_empty_fresh_cut_closes_new_baseline() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let client = Box::pin(deleted_population_fixture(root)).await;
    let original = std::fs::read(root.join("delete-last.json")).unwrap();
    make_fresh_zero_cut(
        root,
        "empty.json",
        "empty-plan",
        "empty-idmap.jsonl",
        "fresh",
    );
    let mut fresh = fresh_args(root, "fresh-empty");
    fresh.baseline_plan = root.join("empty-plan/plan.json");
    fresh.baseline_report = root.join("delete-last.json");
    fresh.baseline_ledger = root.join("delete-last.catchup.sqlite");
    fresh.baseline_receipts = root.join("delete-last.catchup-bindings.jsonl");
    run_inner(fresh, Some(client.clone()), DeltaFault::None)
        .await
        .unwrap();
    let report = header(&root.join("fresh-empty.json")).unwrap();
    assert_eq!(report["changes"], 0);
    assert_eq!(report["batches"], 0);
    assert_eq!(report["regeneration_steps"], 0);
    assert_eq!(report["catchup_baseline"]["bindings_count"], 0);
    assert_eq!(
        report["source_sha256"],
        file_sha(&root.join("fresh.json")).unwrap()
    );
    assert_eq!(
        report["state"],
        "history_reconciled_current_embedding_coverage_pending"
    );
    assert_eq!(
        std::fs::read(root.join("delete-last.json")).unwrap(),
        original
    );
    assert!(client.recovery_status().ready);
    // A successor empty baseline must itself remain certified and usable. An
    // arbitrary report containing the same empty list cannot manufacture it.
    let mut forged = report.clone();
    forged["migration_id"] = "f".repeat(64).into();
    std::fs::write(
        root.join("forged-empty.json"),
        serde_json::to_vec(&forged).unwrap(),
    )
    .unwrap();
    let next = |attempt: &str| {
        let mut command = fresh_args(root, attempt);
        command.baseline_plan = root.join("fresh-plan/plan.json");
        command.baseline_report = root.join("fresh-empty.json");
        command.baseline_ledger = root.join("fresh-empty.catchup.sqlite");
        command.baseline_receipts = root.join("fresh-empty.catchup-bindings.jsonl");
        command.delta_plan = root.join("fresh-noop/delta.json");
        command
    };
    let mut forged_command = next("forged-empty-noop");
    forged_command.baseline_report = root.join("forged-empty.json");
    assert!(
        run_inner(forged_command, Some(client.clone()), DeltaFault::None)
            .await
            .is_err()
    );
    assert!(client.recovery_status().ready);
    assert!(!root
        .join("journal/historical-catchup-pending.json")
        .exists());
    run_inner(
        next("fresh-empty-noop"),
        Some(client.clone()),
        DeltaFault::None,
    )
    .await
    .unwrap();
    let unchanged = header(&root.join("fresh-empty-noop.json")).unwrap();
    assert_eq!(unchanged["state"], "verified_unchanged_history");
    assert_eq!(unchanged["catchup_baseline"], report["catchup_baseline"]);
}

fn make_inherited_operational_delta(root: &Path) {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let code = r"
import copy,json,sys,uuid
from pathlib import Path
sys.path.insert(0,str(Path(sys.argv[1])/'scripts/migration'))
import preservation_plan as p
root=Path(sys.argv[2]); old=json.loads((root/'fresh.json').read_text()); new=copy.deepcopy(old)
a=copy.deepcopy(new['tables']['work_item'][0]);b=new['tables']['work_item'][0];b['payload']['after_untouched_cut']=True
new_event=copy.deepcopy(new['tables']['system_event'][0]);new_event['id']='system_event:'+str(uuid.uuid5(uuid.NAMESPACE_URL,'fresh-cut-event'));new_event['seq']=18;new_event['payload']={'original_s2':True};new['tables']['system_event'].append(new_event)
new['exported_at']='2026-10-15T02:00:00Z';snapshot=root/'operational-s2.json';snapshot.write_bytes(p.canonical(new))
p.inventory(snapshot,root/'idmap.jsonl',root/'operational-s2-plan',space='default',target_space='household',snapshot_sha256=p.file_sha(snapshot),idmap_sha256=p.file_sha(root/'idmap.jsonl'))
def change(table,a,b):
 return {'table':table,'key':(a or b)['id'],'class':'operational','kind':p.change_kind(a,b),'flags':[],'changed_fields':sorted(k for k in set(a or {})|set(b or {}) if (a or {}).get(k)!=(b or {}).get(k)), 'dependency_rank':1,'updated_at':{'s0':(a or {}).get('updated_at'),'s1':(b or {}).get('updated_at')},'deleted_at':{'s0':(a or {}).get('deleted_at'),'s1':(b or {}).get('deleted_at')},'row_sha256':{'s0':None if a is None else p.value_hash(a),'s1':None if b is None else p.value_hash(b)},'before':a,'after':b}
changes=root/'operational-changes.jsonl';changes.write_bytes(p.canonical(change('work_item',a,b))+b'\n'+p.canonical(change('system_event',None,new_event))+b'\n')
plan=json.loads((root/'fresh-plan/plan.json').read_text())
p.delta(root/'fresh-plan/inventory.sqlite',changes,snapshot,root/'operational-delta',before_snapshot_sha256=plan['snapshot_sha256'],after_snapshot_sha256=p.file_sha(snapshot),changes_sha256=p.file_sha(changes),baseline_sha256=plan['inventory_sha256'])
";
    let output = std::process::Command::new("python3")
        .args(["-c", code])
        .arg(workspace)
        .arg(root)
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
fn inherited_args(root: &Path, attempt: &str) -> DataMigrateDeltaArgs {
    let mut command = args(root, attempt);
    command.baseline_plan = root.join("fresh-plan/plan.json");
    command.baseline_report = root.join("fresh-complete.json");
    command.baseline_ledger = root.join("fresh-complete.catchup.sqlite");
    command.baseline_receipts = root.join("fresh-complete.catchup-bindings.jsonl");
    command.delta_plan = root.join("operational-delta/delta.json");
    command.s1_plan = root.join("operational-s2-plan/plan.json");
    command
}
async fn inherited_fixture(root: &Path) -> StorageClient {
    let client = initial_population_fixture(root).await;
    make_fresh_zero_cut(root, "snapshot.json", "plan", "idmap.jsonl", "fresh");
    run_inner(
        fresh_args(root, "fresh-complete"),
        Some(client.clone()),
        DeltaFault::None,
    )
    .await
    .unwrap();
    make_inherited_operational_delta(root);
    client
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "The original fixture client and derived repository handles retain native custody across this complete interruption/recovery scenario, including final receipt, epoch, byte and readiness assertions."
)]
async fn command_inherited_hold_and_event_cut_use_completed_baseline_and_replay() {
    use elephant_storage::Repository as _;
    #[derive(serde::Deserialize)]
    struct Checkpoint {
        id: String,
        space_id: String,
        consumer: String,
        seq: i64,
        snapshot: String,
    }
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let client = Box::pin(inherited_fixture(root)).await;
    let initial_sha = file_sha(&root.join("snapshot.json")).unwrap();
    let mut response = client
        .query_checked("SELECT VALUE payload FROM work_producer_origin WHERE space_id='household'")
        .await
        .unwrap();
    let old_payloads: Vec<String> = response.take(0).unwrap();
    assert_eq!(old_payloads.len(), 1);
    let old: serde_json::Value = serde_json::from_str(&old_payloads[0]).unwrap();
    assert_eq!(
        old["historical_transfer"]["snapshot_sha256"], initial_sha,
        "fresh cut must not rewrite untouched provenance"
    );

    let mut response = client.query_checked("SELECT type::string(id) AS id,space_id,consumer,seq,historical_baseline.snapshot_sha256 AS snapshot FROM outbox_checkpoint WHERE space_id='household' AND consumer IN ['knowledge_views','truth_maintenance'] ORDER BY consumer").await.unwrap();
    let old_events: Vec<Checkpoint> = response.take(0).unwrap();
    assert_eq!(old_events.len(), 2);
    assert_eq!(
        old_events
            .iter()
            .map(|row| row.consumer.as_str())
            .collect::<Vec<_>>(),
        vec!["knowledge_views", "truth_maintenance"]
    );
    for row in &old_events {
        assert!(row.id.starts_with("outbox_checkpoint:"));
        assert_eq!(row.space_id, "household");
        assert_eq!(row.seq, 17);
        assert_eq!(row.snapshot, initial_sha);
    }
    assert_ne!(old_events[0].id, old_events[1].id);
    // The fixture also contains an independent foreign-space Doc. Initial
    // transfer seals that space's event tail too; its controls are not household
    // authority and must remain byte-identical throughout this catch-up.
    let foreign_query =
        "SELECT VALUE type::string($this) FROM outbox_checkpoint WHERE space_id='foreign'";
    let mut response = client.query_checked(foreign_query).await.unwrap();
    let mut foreign_before: Vec<String> = response.take(0).unwrap();
    foreign_before.sort();
    assert_eq!(foreign_before.len(), 3); // two consumers and the sequence floor
    let error = run_inner(
        inherited_args(root, "operational-gap"),
        Some(client.clone()),
        DeltaFault::AfterDatabaseCommit,
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("after DB commit before ledger"),
        "{error:#}"
    );
    assert!(!client.recovery_status().ready);
    run_inner(
        inherited_args(root, "operational-complete"),
        Some(client.clone()),
        DeltaFault::None,
    )
    .await
    .unwrap();
    let mut response = client
        .query_checked("SELECT VALUE payload FROM work_producer_origin WHERE space_id='household'")
        .await
        .unwrap();
    let payloads: Vec<String> = response.take(0).unwrap();
    let current: serde_json::Value = serde_json::from_str(&payloads[0]).unwrap();
    assert_eq!(
        current["historical_transfer"]["snapshot_sha256"],
        file_sha(&root.join("operational-s2.json")).unwrap()
    );
    assert_eq!(
        current["historical_transfer"]["previous_hold_sha256"],
        hex::encode(Sha256::digest(old_payloads[0].as_bytes()))
    );
    let raw: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("operational-s2.json")).unwrap()).unwrap();
    let mut expected: elephant_types::records::WorkItem =
        serde_json::from_value(raw["tables"]["work_item"][0].clone()).unwrap();
    expected.base.space_id = "household".into();
    let work = elephant_storage::WorkItemRepository::new(client.clone());
    assert_eq!(
        work.get("household", &expected.base.id).await.unwrap(),
        Some(expected.clone())
    );
    assert!(work
        .claim_with_lease(
            "household",
            &expected.base.id,
            "forbidden-history",
            chrono::Utc::now() + chrono::Duration::minutes(1)
        )
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        work.get("household", &expected.base.id).await.unwrap(),
        Some(expected)
    );
    let checkpoints =
        elephant_storage::outbox_checkpoint_repository::OutboxCheckpointRepository::new(
            client.clone(),
        );
    for consumer in ["knowledge_views", "truth_maintenance"] {
        assert_eq!(checkpoints.load("household", consumer).await.unwrap(), 18);
    }
    assert!(elephant_storage::SystemEventRepository::new(client.clone())
        .get_after_seq("household", 18, 100)
        .await
        .unwrap()
        .is_empty());
    let mut response = client.query_checked(foreign_query).await.unwrap();
    let mut foreign_after: Vec<String> = response.take(0).unwrap();
    foreign_after.sort();
    assert_eq!(foreign_after, foreign_before);
    let report = header(&root.join("operational-complete.json")).unwrap();
    assert_eq!(report["changes"], 2);
    assert!(client.recovery_status().ready);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "The original fixture client and derived repository handles retain native custody across this complete interruption/recovery scenario, including final receipt, epoch, byte and readiness assertions."
)]
async fn command_inherited_event_control_same_epoch_unknown_field_refuses() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let client = Box::pin(inherited_fixture(root)).await;
    let canonical = elephant_storage::canonical_commit::CanonicalRepository::new(client.clone());
    let epoch = canonical.observe_epoch("household").await.unwrap();
    client.query_checked("UPDATE outbox_checkpoint SET inherited_unknown='tampered' WHERE consumer='knowledge_views'").await.unwrap();
    assert_eq!(
        canonical.observe_epoch("household").await.unwrap(),
        epoch,
        "same-epoch fixture"
    );
    let error = run_inner(
        inherited_args(root, "operational-tampered"),
        Some(client.clone()),
        DeltaFault::None,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("historical completion binding differs"),
        "{error:#}"
    );
    assert!(!client.recovery_status().ready);
    assert!(!root.join("operational-tampered.json").exists());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "The original fixture client and derived repository handles retain native custody across this complete interruption/recovery scenario, including final receipt, epoch, byte and readiness assertions."
)]
async fn command_inherited_control_refuses_foreign_completed_certificate() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let client = Box::pin(inherited_fixture(root)).await;
    let sha = file_sha(&root.join("fresh-complete.json")).unwrap();
    let certificate = root
        .join("journal/historical-catchup-completed")
        .join(format!("{sha}.baseline.json"));
    let original = std::fs::read(&certificate).unwrap();
    let mut foreign: serde_json::Value = serde_json::from_slice(&original).unwrap();
    foreign["marker"]["journal_identity"] = "foreign-journal".into();
    std::fs::write(&certificate, serde_json::to_vec(&foreign).unwrap()).unwrap();
    assert!(run_inner(
        inherited_args(root, "operational-forged"),
        Some(client.clone()),
        DeltaFault::None
    )
    .await
    .is_err());
    assert!(!client.recovery_status().ready);
    assert!(!root.join("operational-forged.json").exists());
    std::fs::write(&certificate, original).unwrap();
    run_inner(
        inherited_args(root, "operational-restored"),
        Some(client.clone()),
        DeltaFault::None,
    )
    .await
    .unwrap();
    assert!(client.recovery_status().ready);
}

#[path = "historical_archive_delta_tests.rs"]
mod archive_tests;
