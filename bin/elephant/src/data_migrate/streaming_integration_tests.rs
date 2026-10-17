//! Actual Python inventory through native schema, copy, adoption, hold and baseline.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use elephant_types::records::doc_node::{NodeRole, TextSpan};
use elephant_types::records::doc_revision::{RendererMetadata, RevisionStatus};
use elephant_types::records::entity::EntityType;
use elephant_types::records::{BaseFields, SystemEvent, WorkItem, WorkItemStatus};
use elephant_types::records::{Doc, DocNode, DocRevision, Entity, NodeEmbedding};
use elephant_types::{Link, RecordId, TableName};

pub fn planner_fixture(root: &Path) -> std::path::PathBuf {
    planner_fixture_mode(root, "native")
}

pub fn planner_fixture_mode(root: &Path, disposition: &str) -> std::path::PathBuf {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let base = |table| {
        BaseFields::new(
            RecordId::new_v4(table),
            "default".into(),
            "synthetic-original".into(),
        )
    };
    let work = WorkItem {
        base: base(TableName::WorkItem),
        kind: "extract_doc".into(),
        payload: serde_json::json!({"space_id":"default","unchanged":true}),
        status: WorkItemStatus::Runnable,
        priority: 1,
        idempotency_key: "synthetic-original".into(),
        attempts: 2,
        leased_by: None,
        lease_expires_at: None,
        run_after: None,
        last_error: Some("original failure".into()),
    };
    let event = SystemEvent {
        base: base(TableName::SystemEvent),
        seq: 17,
        event_type: "records.changed".into(),
        payload: serde_json::json!({"original":true}),
    };
    let raw: serde_json::Value = serde_json::from_slice(
        &std::fs::read(workspace.join("scripts/migration/fixtures/python-raw-documents-v1.json"))
            .unwrap(),
    )
    .unwrap();
    let mut revisions = Vec::new();
    let mut nodes = Vec::new();
    for case in raw["cases"].as_array().unwrap() {
        let doc: Doc = serde_json::from_value(case["original"].clone()).unwrap();
        let mut revision_base = doc.base.clone();
        revision_base.id = RecordId::new_v4(TableName::DocRevision);
        revision_base.space_id = "default".into();
        let revision = DocRevision {
            base: revision_base,
            doc: Link::new(doc.base.id),
            text: doc.text.clone(),
            text_hash: hex::encode(Sha256::digest(doc.text.as_bytes())),
            renderer: RendererMetadata {
                name: "original".into(),
                version: "synthetic-v1".into(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: doc.base.created_at,
        };
        let mut node_base = revision.base.clone();
        node_base.id = RecordId::new_v4(TableName::DocNode);
        nodes.push(DocNode {
            base: node_base,
            revision: Link::new(revision.base.id.clone()),
            role: NodeRole::Chunk,
            span: TextSpan::new(0, u32::try_from(doc.text.len()).unwrap()),
            node_key: "original".into(),
            text: None,
            parent: None,
            position: 0,
            depth: 0,
            embedding_model: None,
            collection: None,
        });
        revisions.push(revision);
    }
    let mut entity = Entity::new(
        base(TableName::Entity),
        EntityType::new("person"),
        "Synthetic original".into(),
    );
    entity.metadata = serde_json::json!({"name_embedding":[f64::from(0.1_f32),-0.0_f64,f64::from(f32::from_bits(1))],"name_embedding_model":"legacy-fixture","name_embedding_text_hash":"original-text-hash"});
    let embedding = NodeEmbedding {
        base: base(TableName::ChunkEmbedding),
        node: Link::new(nodes[0].base.id.clone()),
        model: "legacy-fixture".into(),
        dims: 3,
        vector: vec![0.1_f32, -0.0_f32, f32::from_bits(1)],
    };
    let mut embedding_json = serde_json::to_value(&embedding).unwrap();
    embedding_json["vector"] = serde_json::json!(embedding
        .vector
        .iter()
        .map(|v| f64::from(*v))
        .collect::<Vec<_>>());
    std::fs::write(
        root.join("operational.json"),
        serde_json::to_vec(&serde_json::json!({"work_item":[work],"system_event":[event],"doc_revision":revisions,"doc_node":nodes,"entity":[entity],"chunk_embedding":[embedding_json]}))
            .unwrap(),
    )
    .unwrap();
    let code = r"
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path(sys.argv[1]) / 'scripts/migration'))
import preservation_plan as p
root = Path(sys.argv[2])
fixture = json.loads((Path(sys.argv[1]) / 'scripts/migration/fixtures/python-raw-documents-v1.json').read_text())
rows = [case['original'] for case in fixture['cases']]
for doc in rows:
    doc['space_id'] = 'default'
    doc['rendition_key'] = doc['id']
foreign = dict(rows[0], id='doc:00000000-0000-4000-8000-000000000003', space_id='foreign', text='foreign original text')
# This independent foreign document has its own rendition identity. Keeping the
# copied default-document ID would correctly trip conservative relocation refusal.
foreign['rendition_key'] = foreign['id']
tables = json.loads((root / 'operational.json').read_text())
tables['doc'] = rows + [foreign]
if sys.argv[3] == 'managed_archive_v1':
    tables['work_edge'] = []
    tables['system_event'].append(dict(tables['system_event'][0], id='system_event:00000000-0000-4000-8000-000000000099',space_id='foreign',seq=29))
snapshot = root / 'snapshot.json'
idmap = root / 'idmap.jsonl'
snapshot.write_bytes(p.canonical({'backend':'elephant-db','version':1,'exported_at':'2026-10-10T00:00:00Z','tables':tables}))
idmap.write_bytes(b''.join(p.canonical({'old_doc_id':doc['id'],'class':'source_less','content_sha256':p.value_hash({'title':doc['title'],'text':doc['text']})}) + b'\n' for doc in rows))
p.inventory(snapshot, idmap, root / 'plan', space='default', target_space='household', snapshot_sha256=p.file_sha(snapshot), idmap_sha256=p.file_sha(idmap), operational_disposition=sys.argv[3])
";
    let result = std::process::Command::new("python3")
        .args(["-c", code])
        .arg(&workspace)
        .arg(root)
        .arg(disposition)
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "planner: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    root.join("plan/plan.json")
}

pub fn write_config(root: &Path) -> std::path::PathBuf {
    let config = serde_json::json!({"method":"POST","path":"/v1/spaces","headers":{"X-Space-Id":"household"},"body":{
        "name":"household","security":{"policy_id":"fixture-policy","level":0},
        "embedding_provider":"openai","embedding_model":"fixture","embedding_dims":3,
        "processing":{"generation":1,"index_generation":0,"policy_id":"fixture-policy",
            "connections":{"fixture":{"id":"fixture","provider":"openai","account_id":"unused","endpoint":"https://example.test/v1","credential_ref":"UNUSED","credential_generation":1,"revoked":false}},
            "profiles":{"embedding":{"id":"embedding","connection_id":"fixture","model":"fixture","model_version":"fixture-v1","dimensions":3,"preprocessing":"text-full-v2"}},
            "bindings":{"document_embedding":{"id":"document","profile_id":"embedding","revoked":false},"query_embedding":{"id":"query","profile_id":"embedding","revoked":false}},
            "active_embedding_profile_id":"embedding","pending_reindex":null}
    }});
    let path = root.join("space-request.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    path
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original client and checkpoint reader through complete mapping/hold/event/vector assertions and final journal-ready checks."
)]
async fn actual_python_spool_initial_transfer_reconciles_mapping_hold_events_and_publication() {
    #[derive(Deserialize)]
    struct VectorRow {
        vector: Vec<f32>,
    }
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let plan = planner_fixture(root);
    let config = write_config(root);
    let scratch = root.join("checks.sqlite");
    let report = root.join("report.json");
    let receipts = root.join("adoption.jsonl");
    let spool = SourceSpool::open(&plan, &scratch).unwrap();
    let adoption = super::super::historical_adopt::prepare(
        &spool,
        &plan,
        &config,
        &root.join("adoption.sqlite"),
    )
    .unwrap();
    let digest = spool.source_sha256.clone();
    let expected_docs = spool.page("doc", None).unwrap();
    let expected_work = spool.page("work_item", None).unwrap()[0].lexical.clone();
    let expected_event = spool.page("system_event", None).unwrap()[0].lexical.clone();
    let expected_entity = spool.page("entity", None).unwrap()[0].lexical.clone();
    let expected_embedding = spool.page("chunk_embedding", None).unwrap()[0]
        .lexical
        .clone();
    let dst = StorageClient::new_memory().await.unwrap();
    let saved_report = report.clone();
    let saved_scratch = scratch.clone();
    dst.with_transfer_barrier(&digest, move |exclusive| async move {
        copy(&spool, &exclusive, &scratch, &report, adoption, &receipts)
            .await
            .map_err(|e| elephant_storage::StorageError::DataIntegrityError(format!("{e:#}")))?;
        Ok(())
    })
    .await
    .unwrap();
    let output: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&saved_report).unwrap()).unwrap();
    assert_eq!(
        output["state"],
        "initial_history_reconciled_vector_coverage_pending"
    );
    assert_eq!(output["adoption"]["groups"], 3);
    let baseline = &output["catchup_baseline"];
    assert_eq!(output["operational_disposition"], "native");
    assert_eq!(baseline["operational_disposition"], "native");
    assert!(output.get("operational_archive").is_none());
    assert!(baseline.get("operational_archive").is_none());
    assert_eq!(baseline["bindings_count"], 3);
    assert_eq!(
        baseline["bindings_sha256"],
        source_spool::file_sha(&saved_report.with_extension("catchup-bindings.jsonl")).unwrap()
    );
    assert_eq!(
        baseline["expected_ledger_sha256"],
        source_spool::file_sha(&saved_scratch).unwrap()
    );
    assert_eq!(
        baseline["native_rows_sha256"],
        output["destination_native_rows_sha256"]
    );
    assert_eq!(
        baseline["journal_identity"],
        dst.erasure_journal().unwrap().identity()
    );
    assert_eq!(
        baseline["expected_epoch"],
        elephant_storage::canonical_commit::CanonicalRepository::new(dst.clone())
            .observe_epoch("household")
            .await
            .unwrap()
            .unwrap()
    );
    let bindings =
        std::fs::read_to_string(saved_report.with_extension("catchup-bindings.jsonl")).unwrap();
    for line in bindings.lines() {
        let binding: elephant_storage::historical_adoption::HistoricalInitialCatchupSource =
            serde_json::from_str(line).unwrap();
        assert_eq!(
            Some(binding.head),
            elephant_storage::source_publication::SourcePublicationRepository::new(dst.clone())
                .head("household", &binding.source_id)
                .await
                .unwrap()
        );
        assert_eq!(binding.receipt_sha256.len(), 64);
    }

    assert_eq!(output["copied_tables"]["doc"], 3);
    assert_eq!(output["historical_work_holds"]["verified"], true);
    for (table, phases) in output["table_phases"].as_object().unwrap() {
        assert_eq!(phases["rows"], output["copied_tables"][table]);
        let rows = phases["rows"].as_u64().unwrap();
        if rows > 0 {
            assert!(phases["original_lexical_bytes"].as_u64().unwrap() > 0);
            assert!(phases["pages"].as_u64().unwrap() > 0);
        }
        for name in [
            "seconds",
            "spool_page_seconds",
            "project_and_hash_seconds",
            "typed_write_seconds",
            "native_capture_seconds",
        ] {
            let seconds = phases[name].as_f64().unwrap();
            assert!(seconds.is_finite() && seconds >= 0.0);
        }
    }
    assert_eq!(output["historical_work_holds"]["rows"]["work_item"], 1);
    assert_eq!(output["source_space_id"], "default");
    assert_eq!(output["target_space_id"], "household");
    for original in expected_docs {
        let TransferRecord::Doc(raw) = TransferRecord::from_json("doc", &original.lexical).unwrap()
        else {
            panic!("doc")
        };
        let space = if raw.base.space_id == "default" {
            "household"
        } else {
            "foreign"
        };
        let actual = elephant_storage::DocRepository::new(dst.clone())
            .get(space, &raw.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual.text, raw.text);
        assert_eq!(actual.title, raw.title);
        assert_eq!(actual.base.security, raw.base.security);
        if space == "foreign" {
            assert_eq!(actual, raw);
        } else {
            assert!(actual.source.is_some());
        }
    }
    let mut work: WorkItem = serde_json::from_str(&expected_work).unwrap();
    work.base.space_id = "household".into();
    assert_eq!(
        elephant_storage::WorkItemRepository::new(dst.clone())
            .get("household", &work.base.id)
            .await
            .unwrap(),
        Some(work)
    );
    let mut event: SystemEvent = serde_json::from_str(&expected_event).unwrap();
    event.base.space_id = "household".into();
    assert_eq!(
        elephant_storage::SystemEventRepository::new(dst.clone())
            .get("household", &event.base.id)
            .await
            .unwrap(),
        Some(event)
    );
    let checkpoints =
        elephant_storage::outbox_checkpoint_repository::OutboxCheckpointRepository::new(
            dst.clone(),
        );
    for consumer in ["knowledge_views", "truth_maintenance"] {
        assert_eq!(checkpoints.load("household", consumer).await.unwrap(), 17);
        assert!(elephant_storage::SystemEventRepository::new(dst.clone())
            .get_after_seq("household", 17, 100)
            .await
            .unwrap()
            .is_empty());
    }
    assert_eq!(output["entity_name_embedding"], 1);
    assert_eq!(output["copied_tables"]["chunk_embedding"], 1);
    let entity: Entity = serde_json::from_str(&expected_entity).unwrap();
    let mut response = dst
        .query_bind_checked(
            "SELECT name_embedding,name_embedding_model,name_embedding_text_hash FROM $id",
            BTreeMap::from([(
                "id".to_owned(),
                Native::from(elephant_storage::surreal::thing_from_record_id(
                    &entity.base.id,
                )),
            )]),
        )
        .await
        .unwrap();
    let vectors: Vec<super::super::EntityVector> = response.take(0).unwrap();
    assert_eq!(vectors.len(), 1);
    let expected = [0.1_f32, -0.0_f32, f32::from_bits(1)];
    assert_eq!(
        vectors[0]
            .name_embedding
            .iter()
            .map(|v| v.to_bits())
            .collect::<Vec<_>>(),
        expected.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
    );
    assert_eq!(vectors[0].name_embedding_model, "legacy-fixture");
    assert_eq!(vectors[0].name_embedding_text_hash, "original-text-hash");
    let TypedRow::Embedding(mut embedding) =
        source_spool::typed("chunk_embedding", &expected_embedding).unwrap()
    else {
        panic!("embedding")
    };
    embedding.base.space_id = "household".into();
    let partition = super::super::embedding_partition(&embedding).unwrap();
    let mut response = dst
        .query_bind_checked(
            "SELECT vector FROM type::table($table)",
            BTreeMap::from([("table".to_owned(), Native::from(partition))]),
        )
        .await
        .unwrap();
    let rows: Vec<VectorRow> = response.take(0).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]
            .vector
            .iter()
            .map(|v| v.to_bits())
            .collect::<Vec<_>>(),
        expected.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
    );
    dst.verify_journal_identity().await.unwrap();
    assert!(dst.recovery_status().ready);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original client through the entire custody callback and both missing/extra bucket refusal assertions."
)]
async fn copy_fence_seal_refuses_extra_and_missing_buckets() {
    let directory = tempfile::tempdir().unwrap();
    let plan = planner_fixture(directory.path());
    let scratch = directory.path().join("checks.sqlite");
    let spool = SourceSpool::open(&plan, &scratch).unwrap();
    let dst = StorageClient::new_memory().await.unwrap();
    let digest = spool.source_sha256.clone();
    dst.with_transfer_barrier(&digest,move |exclusive|async move {
        exclusive.query_checked(include_str!("../../../../crates/migrations/sql/043_native_read_fence.surql")).await?;
        let docs=spool.page("doc",None).unwrap().iter().map(|row|match spool.projected("doc",&row.lexical).unwrap(){TypedRow::Ordinary(row)=>row,_=>panic!("doc")}).collect::<Vec<_>>();
        import_batch(&exclusive,&spool.source_sha256,&docs).await?;
        let mut checks=Connection::open(scratch).unwrap(); let mut tables=BTreeSet::new();
        seal_copy_physical_fences(&spool,&exclusive,&mut checks,&mut tables).await.unwrap();
        exclusive.query_checked("RETURN fn::recall_physical_read_fence(source_head:synthetic)").await?;
        assert!(seal_copy_physical_fences(&spool,&exclusive,&mut checks,&mut tables).await.is_err());
        exclusive.query_checked("DELETE native_read_fence WHERE string::starts_with(meta::id(id),'source_head:'); DELETE native_read_fence WHERE id=(SELECT VALUE id FROM native_read_fence LIMIT 1)[0]").await?;
        assert!(seal_copy_physical_fences(&spool,&exclusive,&mut checks,&mut tables).await.is_err());
        exclusive.initialize_journal_identity().await?; Ok(())
    }).await.unwrap();
}
