//! Legacy native literal wrappers through the real planner, copy and adoption path.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::source_spool::file_sha;
use super::*;
use elephant_storage::read_eligibility::ReadEligibility;
use elephant_storage::{AssertionRepository, AttributeRepository, Repository};
use elephant_types::records::{Assertion, Attribute};
use std::path::PathBuf;

pub fn python(root: &Path, code: &str) {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = std::process::Command::new("python3")
        .args(["-B", "-c", code])
        .arg(workspace)
        .arg(root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

pub fn fixture(root: &Path) -> PathBuf {
    integration_tests::planner_fixture_mode(root, "managed_archive_v1");
    python(
        root,
        r"
import copy,hashlib,json,sys,uuid
from pathlib import Path
sys.path.insert(0,str(Path(sys.argv[1])/'scripts/migration'))
import preservation_plan as p
r=Path(sys.argv[2]); snap=json.loads((r/'snapshot.json').read_text());t=snap['tables']
rev=t['doc_revision'][0];node=next(n for n in t['doc_node'] if n['revision']==rev['id']);entity=t['entity'][0]
base={k:copy.deepcopy(entity[k]) for k in ('space_id','created_at','updated_at','created_by','security','provenance','subjects','subjects_state') if k in entity}
def key(table,label): return table+':'+str(uuid.uuid5(uuid.NAMESPACE_URL,'legacy-literal:'+label))
values=[('string','original å 🐾'),('integer',9007199254740993),('boolean',False),('float',1.25),('float',1.0),('float',-0.0)]
t['assertion']=[];t['attribute']=[]
for i,(tag,value) in enumerate(values):
 for deleted in (False,True):
  label=(tag if i<4 else tag+'-'+str(i))+('-deleted' if deleted else '-live')
  evidence={'revision':rev['id'],'node':node['id'],'start':0,'end':len(rev['text'].encode()),'quote_hash':hashlib.sha256(rev['text'].encode()).hexdigest()}
  assertion={**copy.deepcopy(base),'id':key('assertion',label),'subject_text':'Synthetic original','predicate_raw':'legacy_'+tag,'object_literal':{'type':tag,'value':value},'evidence':evidence,'observed_at':base['created_at'],'asserted_at':base['created_at'],'assertion_key':label,'subject_entity':entity['id'],'resolution_status':'resolved','retry_state':{'resolution_attempts':0},'lifecycle_tier':'active','confidence':0.5}
  attribute={**copy.deepcopy(base),'id':key('attribute',label),'subject':entity['id'],'attr_type':'legacy_'+tag,'status':'candidate','value':{'type':tag,'value':value},'confidence':0.5,'evidence':[{'assertion':assertion['id'],'span':evidence,'time_basis':'observed_at','time_value':base['created_at']}]}
  if deleted: assertion['deleted_at']=base['updated_at'];attribute['deleted_at']=base['updated_at']
  t['assertion'].append(assertion);t['attribute'].append(attribute)
# An independent original fact has no document evidence; it must use the snapshot anchor.
standalone=copy.deepcopy(t['assertion'][0]);standalone['id']=key('assertion','snapshot-only');standalone['assertion_key']='snapshot-only';standalone['evidence']={'revision':key('doc_revision','absent'),'start':0,'end':0,'quote_hash':''};standalone['object_literal']={'type':'string','value':'source-less original'}
t['assertion'].append(standalone)
snapshot=r/'literal-snapshot.json';snapshot.write_bytes(p.canonical(snap))
p.inventory(snapshot,r/'idmap.jsonl',r/'literal-plan',space='default',target_space='household',snapshot_sha256=p.file_sha(snapshot),idmap_sha256=p.file_sha(r/'idmap.jsonl'),operational_disposition='managed_archive_v1')
",
    );
    root.join("literal-plan/plan.json")
}

fn source_digest(spool: &SourceSpool) -> String {
    let mut hash = Sha256::new();
    for (table, count) in &spool.tables {
        hash.update(serde_json::to_vec(&serde_json::json!(["table", table, count])).unwrap());
        hash.update(b"\n");
    }
    for control in &spool.retained_controls {
        hash.update(serde_json::to_vec(&serde_json::json!(["retained_control", control])).unwrap());
        hash.update(b"\n");
    }
    for table in super::super::SUPPORTED_TABLES {
        if *table == "id_map" {
            continue;
        }
        let mut cursor = None;
        loop {
            let rows = spool.page(table, cursor.as_deref()).unwrap();
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                super::super::archive_copy::hash_source_row(&mut hash, table, row).unwrap();
            }
            cursor = rows.last().map(|row| row.id.clone());
        }
    }
    hex::encode(hash.finalize())
}

pub async fn initial(root: &Path, client: &StorageClient) {
    let plan = fixture(root);
    let config = integration_tests::write_config(root);
    let ledger = root.join("initial.sqlite");
    let report = root.join("initial.json");
    let receipts = root.join("initial.adoption.jsonl");
    let spool = SourceSpool::open(&plan, &ledger).unwrap();
    let expected_source = source_digest(&spool);
    let source_hash = file_sha(&root.join("literal-snapshot.json")).unwrap();
    let inventory_hash = file_sha(&spool.path).unwrap();
    let plan_hash = file_sha(&plan).unwrap();
    let adoption = super::super::historical_adopt::prepare(
        &spool,
        &plan,
        &config,
        &root.join("adoption.sqlite"),
    )
    .unwrap();
    let digest = spool.source_sha256.clone();
    let saved_report = report.clone();
    client
        .with_transfer_barrier(&digest, move |exclusive| async move {
            copy(&spool, &exclusive, &ledger, &report, adoption, &receipts)
                .await
                .map_err(|error| {
                    elephant_storage::StorageError::DataIntegrityError(format!("{error:#}"))
                })
        })
        .await
        .unwrap();
    let result: serde_json::Value =
        serde_json::from_slice(&std::fs::read(saved_report).unwrap()).unwrap();
    assert_eq!(
        result["state"],
        "initial_history_reconciled_vector_coverage_pending"
    );
    assert_eq!(result["source_lexical_rows_sha256"], expected_source);
    assert_eq!(result["copied_tables"]["assertion"], 13);
    assert_eq!(result["copied_tables"]["attribute"], 12);
    assert_eq!(
        result["catchup_baseline"]["native_rows_sha256"],
        result["destination_native_rows_sha256"]
    );
    assert_eq!(
        file_sha(&root.join("literal-snapshot.json")).unwrap(),
        source_hash
    );
    assert_eq!(
        file_sha(&root.join("literal-plan/inventory.sqlite")).unwrap(),
        inventory_hash
    );
    assert_eq!(file_sha(&plan).unwrap(), plan_hash);
    assert!(client.recovery_status().ready);
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep native client and current-read custody through all linked literal projections and final currentness checks."
)]
async fn check_literals(root: &Path, client: &StorageClient) {
    let snapshot: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("literal-snapshot.json")).unwrap())
            .unwrap();
    let reader = ReadEligibility::new(client.clone());
    let assertions = AssertionRepository::new(client.clone());
    let attributes = AttributeRepository::new(client.clone());
    for raw in snapshot["tables"]["assertion"].as_array().unwrap() {
        let original: Assertion = serde_json::from_value(raw.clone()).unwrap();
        let actual = assertions
            .get_with_deleted("household", &original.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            actual.object_literal.as_ref(),
            Some(&raw["object_literal"]["value"])
        );
        if raw["object_literal"]["type"] == "float" {
            assert_eq!(
                actual
                    .object_literal
                    .as_ref()
                    .unwrap()
                    .as_f64()
                    .unwrap()
                    .to_bits(),
                raw["object_literal"]["value"].as_f64().unwrap().to_bits(),
            );
        }
        assert_eq!(actual.base.deleted_at, original.base.deleted_at);
        assert_eq!(actual.base.security, original.base.security);
        assert_eq!(actual.evidence, original.evidence);
        if original.base.deleted_at.is_none() {
            assert!(reader.project(actual, &|_| true).await.unwrap().is_some());
        } else {
            assert!(assertions
                .get("household", &original.base.id)
                .await
                .unwrap()
                .is_none());
        }
        let mut public = original;
        public.base.space_id = "household".into();
        assert!(assertions
            .create(public)
            .await
            .unwrap_err()
            .to_string()
            .contains("reserved"));
    }
    for raw in snapshot["tables"]["attribute"].as_array().unwrap() {
        let original: Attribute = serde_json::from_value(raw.clone()).unwrap();
        let actual = attributes
            .get_with_deleted("household", &original.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual.value, original.value);
        assert_eq!(actual.evidence, original.evidence);
        assert_eq!(actual.base.deleted_at, original.base.deleted_at);
    }
    reader.verify_current().await.unwrap();
}

#[tokio::test]
async fn legacy_native_literals_initial_archive_preserves_lexical_graph_and_public_guard() {
    let directory = tempfile::tempdir().unwrap();
    let client = StorageClient::new_memory().await.unwrap();
    Box::pin(initial(directory.path(), &client)).await;
    Box::pin(check_literals(directory.path(), &client)).await;
}
