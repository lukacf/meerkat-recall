//! Real initial publication -> interrupted S1 -> exact durable replay -> full ledger closure.
//! Synthetic retained-control delta changes no document/vector payload or provider state.
use super::*;
use crate::client::{HistoricalCatchupInput, HistoricalCatchupSource};
use crate::historical_catchup_ledger::HistoricalCatchupLedger;
use crate::historical_delta::{
    retained_control_fingerprint, HistoricalDeltaBatch, HistoricalRetainedControlDelta,
};
use std::path::{Path, PathBuf};

fn s1_plan(rows: &HistoricalGroupRows) -> HistoricalAdoptionInput {
    let original: Doc =
        serde_json::from_value(rows.documents[0].original_document.clone()).unwrap();
    let mut declaration = declaration(&original);
    declaration.external_source_id = "hc-regrouped-original".into();
    let mut input = plan(vec![declaration]);
    input.source_space_id = "default".into();
    input.snapshot_sha256 = "e".repeat(64);
    input.plan_id = "f".repeat(64);
    input.groups[0].external_source_id = "hc-regrouped-original".into();
    input.documents_sha256 = jsonl_sha256(input.groups[0].documents.iter()).unwrap();
    input.groups_sha256 = jsonl_sha256([serde_json::json!({
        "external_source_id": input.groups[0].external_source_id,
        "documents": 1,
        "document_mapping_sha256": input.groups[0].document_mapping_sha256,
    })])
    .unwrap();
    input.validate().unwrap();
    input
}
fn batch(epoch: &str) -> HistoricalDeltaBatch {
    let before = serde_json::json!({"id":"migration_history:fake_embedding_purge_v1",
        "name":crate::fake_embedding_repair::RECEIPT_NAME,
        "checksum":crate::fake_embedding_repair::RECEIPT_CHECKSUM,
        "applied_at":"2026-01-01T00:00:00Z"});
    let mut after = before.clone();
    after["applied_at"] = "2026-01-02T00:00:00Z".into();
    HistoricalDeltaBatch {
        version: 1,
        migration_id: "1".repeat(64),
        source_space_id: "default".into(),
        space_id: "history-test".into(),
        before_snapshot_sha256: "b".repeat(64),
        after_snapshot_sha256: "e".repeat(64),
        planner_delta_sha256: "2".repeat(64),
        batch_ordinal: 0,
        previous_batch_receipt: None,
        expected_epoch: epoch.into(),
        rows: vec![],
        embeddings: vec![],
        retained_controls: vec![HistoricalRetainedControlDelta {
            before_fingerprint: Some(retained_control_fingerprint(&before).unwrap()),
            after_fingerprint: Some(retained_control_fingerprint(&after).unwrap()),
            before: Some(before),
            after: Some(after),
        }],
    }
}

// Only a test creates a baseline from its freshly established S0 fixture. The
// production ledger consumes the immutable original transfer report instead.
pub(super) async fn baseline(
    storage: &StorageClient,
    root: &Path,
) -> (PathBuf, PathBuf, String, String, String) {
    let path = root.join("s0.sqlite");
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch(
        "CREATE TABLE expected(t TEXT,k TEXT,h TEXT,seq INTEGER,PRIMARY KEY(t,k),UNIQUE(t,seq));",
    )
    .unwrap();
    let tables = storage.list_tables().await.unwrap();
    let mut observed = Vec::new();
    for table in &tables {
        let mut response = storage.query_bind_checked(
            "SELECT type::string(id) AS id,crypto::sha256(type::string($this)) AS sha FROM type::table($table) ORDER BY id",
            crate::bindings!{"table"=>table.clone()}).await.unwrap();
        let rows: Vec<serde_json::Value> = response.take(0).unwrap();
        observed.push((table, rows));
    }
    let mut all = Vec::new();
    {
        // Keep the test's single SQLite transaction entirely synchronous. The
        // native observations above retain the same table and row order.
        let tx = db.unchecked_transaction().unwrap();
        for (table, rows) in observed {
            for (seq, row) in rows.iter().enumerate() {
                let id = row["id"].as_str().unwrap();
                let hash = row["sha"].as_str().unwrap();
                tx.execute(
                    "INSERT INTO expected VALUES(?,?,?,?)",
                    rusqlite::params![table, id, hash, i64::try_from(seq).unwrap()],
                )
                .unwrap();
                all.push(serde_json::json!([table, id, hash]));
            }
        }
        tx.commit().unwrap();
    }
    drop(db);
    let ledger_sha = sha256(&std::fs::read(&path).unwrap());
    let native_sha = jsonl_sha256(all).unwrap();
    let report = serde_json::json!({"format":"recall-legacy-transfer-v2",
    "state":"initial_history_reconciled_vector_coverage_pending",
    "destination_native_rows_sha256":native_sha,
    "catchup_baseline":{
        "format":"recall-initial-catchup-bindings-v1",
        "expected_ledger_sha256":ledger_sha,"native_rows_sha256":native_sha,
        "journal_identity":storage.erasure_journal().unwrap().identity(),"expected_tables":tables,
        "expected_epoch":CanonicalRepository::new(storage.clone()).observe_epoch("history-test").await.unwrap()
    }});
    let report_path = root.join("s0-report.json");
    std::fs::write(&report_path, serde_json::to_vec(&report).unwrap()).unwrap();
    let report_sha = sha256(&std::fs::read(&report_path).unwrap());
    (report_path, path, report_sha, ledger_sha, native_sha)
}

pub(super) struct InterruptedS1 {
    rows: HistoricalGroupRows,
    receipt: HistoricalAdoptionReceipt,
    bindings: Vec<HistoricalCatchupSource>,
    canonical: crate::canonical_commit::CanonicalCommitReceipt,
    input: HistoricalAdoptionInput,
    ordered: HistoricalRegenerationPlan,
    epoch: String,
    saved_declaration: Vec<u8>,
    report: PathBuf,
    ledger: PathBuf,
}

#[allow(
    clippy::too_many_lines,
    reason = "Keep the complete ordered fixture and its failure/recovery assertions together."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve sealed-authority transfer, the distinct Result and prepared owners, and injected post-commit interruption teardown; never shorten the original pending custody."
)]
pub(super) async fn exercise_s1(
    storage: StorageClient,
    rows: HistoricalGroupRows,
    receipt: HistoricalAdoptionReceipt,
    embedded: bool,
    root: &Path,
) -> Option<InterruptedS1> {
    let publications = SourcePublicationRepository::new(storage.clone());
    let head = publications
        .head("history-test", &receipt.source_id)
        .await
        .unwrap()
        .unwrap();
    let canonical = CanonicalRepository::new(storage.clone())
        .receipt_read_only("history-test", &receipt.commit_id)
        .await
        .unwrap()
        .unwrap();
    let bindings = vec![HistoricalCatchupSource {
        source_id: receipt.source_id.clone(),
        commit_id: receipt.commit_id.clone(),
        receipt_sha256: sha256(&serde_json::to_vec(&canonical).unwrap()),
        head,
    }];
    let input = s1_plan(&rows);
    let ordered = HistoricalRegenerationPlan::derive(&input, &bindings).unwrap();
    assert_eq!(ordered.steps.len(), 2);
    assert!(ordered.steps[0].external_source_id.is_some());
    assert!(ordered.steps[1].external_source_id.is_none());
    let epoch = CanonicalRepository::new(storage.clone())
        .observe_epoch("history-test")
        .await
        .unwrap()
        .unwrap();
    let (report, ledger, report_sha, ledger_sha, native_sha) = if embedded {
        (
            root.join("unused-report"),
            root.join("unused-ledger"),
            "3".repeat(64),
            "4".repeat(64),
            "5".repeat(64),
        )
    } else {
        baseline(&storage, root).await
    };
    let declaration = HistoricalCatchupInput {
        version: 1,
        journal_identity: storage.erasure_journal().unwrap().identity().into(),
        migration_id: "1".repeat(64),
        source_space_id: "default".into(),
        space_id: "history-test".into(),
        before_snapshot_sha256: "b".repeat(64),
        after_snapshot_sha256: "e".repeat(64),
        delta_sha256: "2".repeat(64),
        original_report_sha256: report_sha,
        baseline_ledger_sha256: ledger_sha,
        baseline_native_rows_sha256: native_sha,
        accept_deletions: false,
        expected_epoch: epoch.clone(),
        batches: 1,
        regeneration_plan_sha256: ordered.plan_sha256.clone(),
        regeneration_steps: 2,
        sources: bindings.clone(),
    };
    if embedded {
        let unchanged_before = crate::DocRepository::new(storage.clone())
            .get("history-test", &rows.documents[0].document.base.id)
            .await
            .unwrap();
        let result = storage
            .with_historical_catchup(declaration, |_| async move {
                panic!("embedded S1 must refuse before callback/marker/mutation");
            })
            .await;
        assert!(result.is_err());
        assert!(storage.recovery_status().ready);
        assert_eq!(
            Some(epoch),
            CanonicalRepository::new(storage.clone())
                .observe_epoch("history-test")
                .await
                .unwrap()
        );
        assert_eq!(
            unchanged_before,
            crate::DocRepository::new(storage.clone())
                .get("history-test", &rows.documents[0].document.base.id)
                .await
                .unwrap()
        );
        assert!(!root
            .join("journal/historical-catchup-pending.json")
            .exists());
        return None;
    }
    let saved_declaration = serde_json::to_vec(&declaration).unwrap();
    let first_input = input.clone();
    let first_rows = rows.clone();
    let first_bindings = bindings.clone();
    let first_batch = batch(&epoch);
    let first_report = report.clone();
    let first_ledger = ledger.clone();
    let ledger_copy = root.join("attempt-1.sqlite");
    let result = storage
        .with_historical_catchup(declaration, move |mut custody| async move {
            let index = PreparedHistoricalEffectIndex::prepare(&custody, &first_bindings).await?;
            let mut expected = if embedded {
                None
            } else {
                Some(HistoricalCatchupLedger::open(
                    &custody,
                    &first_report,
                    &first_ledger,
                    &ledger_copy,
                )?)
            };
            let (_, effects) = custody
                .apply_batch_with_effects(&index, first_batch.prepare()?)
                .await?;
            if let Some(ledger) = &mut expected {
                ledger.apply(effects.unwrap())?;
            }
            let sealed = custody.seal_for_regeneration().await?;
            let client = sealed.client()?.clone();
            let before = CanonicalRepository::new(client.clone())
                .observe_epoch("history-test")
                .await?;
            let prepared = HistoricalAdoptionRepository::new(client.clone())
                .prepare_catchup_authority(first_input, sealed)
                .await;
            if embedded {
                assert!(prepared
                    .unwrap_err()
                    .to_string()
                    .contains("requires the native backend"));
                assert_eq!(
                    before,
                    CanonicalRepository::new(client)
                        .observe_epoch("history-test")
                        .await?
                );
                return Err(integrity("expected embedded capability refusal"));
            }
            let prepared = prepared?;
            prepared
                .adopt_group("hc-regrouped-original", first_rows)
                .await?;
            // Deliberately omit the external ledger update after the actual DB COMMIT.
            assert!(prepared
                .next_regeneration_step()?
                .unwrap()
                .external_source_id
                .is_none());
            Err(integrity(
                "injected process interruption after first regeneration",
            ))
        })
        .await;
    let error = result.expect_err("injected post-commit interruption must be reached");
    assert_eq!(
        error.to_string(),
        "Data integrity error: injected process interruption after first regeneration",
        "unexpected error before the selected post-commit interruption: {error}"
    );
    assert!(!storage.recovery_status().ready);
    if embedded {
        return None;
    }
    drop(publications);
    drop(storage);
    Some(InterruptedS1 {
        rows,
        receipt,
        bindings,
        canonical,
        input,
        ordered,
        epoch,
        saved_declaration,
        report,
        ledger,
    })
}

// Called only after the first runtime (including the Surreal router task) has
// been destroyed. No live storage handle crosses this process-like boundary.
#[allow(
    clippy::too_many_lines,
    reason = "Keep the complete ordered fixture and its failure/recovery assertions together."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original reopened client through ledger replay, consuming regeneration closure, three-cut lineage and final forged-retirement refusal."
)]
pub(super) async fn resume_s1(interrupted: InterruptedS1, root: &Path) {
    let InterruptedS1 {
        rows,
        receipt,
        bindings,
        canonical,
        input,
        ordered,
        epoch,
        saved_declaration,
        report,
        ledger,
    } = interrupted;
    let storage = StorageClient::new_with_durability(
        StorageConfig {
            backend: crate::client::StorageBackendKind::SurrealDb,
            url: format!("rocksdb://{}", root.join("database").display()),
            database: "historical_s1".into(),
            ..StorageConfig::default()
        },
        None,
        Some(root.join("journal")),
    )
    .await
    .unwrap();
    assert!(!storage.recovery_status().ready);
    let declaration: HistoricalCatchupInput = serde_json::from_slice(&saved_declaration).unwrap();
    let ledger_copy = root.join("attempt-2.sqlite");
    let batch = batch(&epoch);
    let old_source = receipt.source_id.clone();
    let graph_after = rows.graph.clone();
    let third_rows = rows.clone();
    let second_source = ordered.steps[0].source_id.clone();
    storage
        .with_historical_catchup(declaration, move |custody| async move {
            assert_eq!(custody.next_batch().0, 1);
            let index = PreparedHistoricalEffectIndex::prepare(&custody, &bindings).await?;
            let mut expected =
                HistoricalCatchupLedger::open(&custody, &report, &ledger, &ledger_copy)?;
            expected.apply(
                custody
                    .replay_batch_effects(&index, batch.prepare()?)
                    .await?,
            )?;
            let sealed = custody.seal_for_regeneration().await?;
            let client = sealed.client()?.clone();
            let prepared = HistoricalAdoptionRepository::new(client.clone())
                .prepare_catchup_authority(input, sealed)
                .await?;
            let first = prepared.recovered_regeneration_effects(0).await?;
            assert!(!first.effects().is_empty());
            expected.apply(first)?;
            assert!(prepared.recovered_regeneration_effects(1).await.is_err());
            assert_eq!(
                prepared.next_regeneration_step()?.unwrap().source_id,
                old_source
            );
            let (retired, _) = prepared.retire_next_group_with_effects().await?;
            assert_eq!(retired.operation.state, OperationState::Retracted);
            expected.apply(prepared.recovered_regeneration_effects(1).await?)?;
            assert!(prepared.next_regeneration_step()?.is_none());
            let closure = prepared.close_regeneration().await?;
            assert!(
                crate::read_eligibility::ReadEligibility::new(client.clone())
                    .project(rows.documents[0].nodes[0].clone(), &|_| true)
                    .await?
                    .is_some()
            );
            for graph in &rows.graph {
                assert_graph_visible(&client, graph, &|_| true, true).await;
                // Denying the newly adopted source denies every graph kind.
                assert_graph_visible(
                    &client,
                    graph,
                    &|base| base.id.table() != TableName::Source,
                    false,
                )
                .await;
                let mut erased = graph.clone();
                erased.base_mut().deleted_at = Some(Utc::now());
                assert_graph_visible(&client, &erased, &|_| true, false).await;
                let mut foreign = graph.clone();
                foreign.base_mut().space_id = "foreign".into();
                assert_graph_visible(
                    &client,
                    &foreign,
                    &|base| base.space_id == "history-test",
                    false,
                )
                .await;
                let support = SourcePublicationRepository::new(client.clone())
                    .support_for_record("history-test", &graph.base().id)
                    .await?;
                assert_eq!(support.len(), 2);
                assert!(support
                    .iter()
                    .all(|entry| !entry.independent && entry.historical_unknown.is_some()));
            }
            // Retired publication replay correctly refuses. Retention is a
            // separate exact immutable-control equality, never current authority.
            assert!(CanonicalRepository::new(client.clone())
                .receipt_read_only("history-test", &canonical.commit_id)
                .await
                .is_err());
            let retained: crate::canonical_commit::CanonicalCommitReceipt =
                SourcePublicationRepository::new(client)
                    .read_control(
                        "source_commit_receipt",
                        &crate::source_publication::stable_key(&[
                            "history-test",
                            &canonical.commit_id,
                        ]),
                        "history-test",
                    )
                    .await?
                    .unwrap();
            assert_eq!(retained, canonical);
            expected.complete(closure).await
        })
        .await
        .unwrap();
    assert!(storage.recovery_status().ready);
    let refresh_root = root.join("refresh");
    std::fs::create_dir(&refresh_root).unwrap();
    let unchanged_doc_id = third_rows.documents[0].document.base.id.clone();
    let mut before_doc = storage
        .query_bind_checked(
            "SELECT * FROM ONLY $id",
            crate::bindings! {"id"=>crate::surreal::thing_from_record_id(&unchanged_doc_id)},
        )
        .await
        .unwrap();
    let before_doc: surrealdb::Value = before_doc.take(0).unwrap();
    regroup_again(
        &storage,
        third_rows.clone(),
        second_source.clone(),
        &refresh_root,
        true,
    )
    .await;
    let mut after_doc = storage
        .query_bind_checked(
            "SELECT * FROM ONLY $id",
            crate::bindings! {"id"=>crate::surreal::thing_from_record_id(&unchanged_doc_id)},
        )
        .await
        .unwrap();
    let after_doc: surrealdb::Value = after_doc.take(0).unwrap();
    assert_eq!(
        before_doc, after_doc,
        "same-source refresh preserves the entire native Doc image"
    );
    for graph in &graph_after {
        assert_graph_visible(&storage, graph, &|_| true, true).await;
    }
    let third_root = root.join("third");
    std::fs::create_dir(&third_root).unwrap();
    let intermediate_retirement = regroup_again(
        &storage,
        third_rows,
        second_source.clone(),
        &third_root,
        false,
    )
    .await;
    for graph in &graph_after {
        assert_graph_visible(&storage, graph, &|_| true, true).await;
        // Only the intermediate retired B is denied: A and current C remain allowed.
        assert_graph_visible(&storage, graph, &|base| base.id != second_source, false).await;
        assert_eq!(
            SourcePublicationRepository::new(storage.clone())
                .support_for_record("history-test", &graph.base().id)
                .await
                .unwrap()
                .len(),
            4
        );
    }
    // Arbitrary new support cannot replace old attribution: corrupt only the
    // retirement mapping after the reconciled fixture closes, then read again.
    let mut forged = CanonicalRepository::new(storage.clone())
        .receipt_read_only("history-test", &intermediate_retirement)
        .await
        .unwrap()
        .unwrap();
    let output = forged.replay_output.as_mut().unwrap();
    output["graph_replacements"] = serde_json::json!({});
    let id = crate::source_publication::control_thing(
        "source_commit_receipt",
        &crate::source_publication::stable_key(&["history-test", &intermediate_retirement]),
    );
    storage.query_bind_checked("UPDATE ONLY $id SET payload=$payload, state=$state",
        crate::bindings!{"id"=>id,"payload"=>serde_json::to_string(&forged).unwrap(),
            "state"=>crate::surreal::json_to_surreal(serde_json::to_value(&forged).unwrap()).unwrap()})
        .await.unwrap();
    for graph in &graph_after {
        assert_graph_visible(&storage, graph, &|_| true, false).await;
    }
}

async fn assert_graph_visible(
    storage: &StorageClient,
    graph: &CanonicalRecord,
    authorize: &crate::read_eligibility::ReadAuthorization<'_>,
    visible: bool,
) {
    let reader = crate::read_eligibility::ReadEligibility::new(storage.clone());
    macro_rules! check {
        ($row:expr) => {{
            let actual = reader.project($row.as_ref().clone(), authorize).await;
            assert_eq!(actual.is_ok_and(|value| value.is_some()), visible);
        }};
    }
    match graph {
        CanonicalRecord::Entity(row) => check!(row),
        CanonicalRecord::Assertion(row) => check!(row),
        CanonicalRecord::Event(row) => check!(row),
        CanonicalRecord::TruthSlot(row) => check!(row),
        _ => panic!("unexpected fixture graph table"),
    }
    drop(reader);
}

#[allow(
    clippy::too_many_lines,
    reason = "Keep the complete ordered fixture and its failure/recovery assertions together."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve original publication reads and consuming regeneration authority until the exact ledger completion returns."
)]
async fn regroup_again(
    storage: &StorageClient,
    rows: HistoricalGroupRows,
    prior_source: RecordId,
    root: &Path,
    refresh: bool,
) -> String {
    let publications = SourcePublicationRepository::new(storage.clone());
    let head = publications
        .head("history-test", &prior_source)
        .await
        .unwrap()
        .unwrap();
    let operation: crate::source_publication::StoredOperation = publications
        .read_control("source_operation", &head.operation_id, "history-test")
        .await
        .unwrap()
        .unwrap();
    let fence = elephant_types::publication::PublicationFence {
        source_id: prior_source.clone(),
        generation: head.generation.clone(),
        revision_id: head.published_revision.clone().unwrap(),
        operation_id: head.operation_id.clone(),
    };
    let manifest = crate::source_publication_manifest::SourcePublicationManifestRepository::new(
        storage.clone(),
    )
    .get("history-test", &fence)
    .await
    .unwrap()
    .unwrap();
    let commit_id = manifest
        .historical_adoption
        .as_ref()
        .unwrap()
        .commit_id(&manifest);
    let canonical = CanonicalRepository::new(storage.clone())
        .receipt_read_only("history-test", &commit_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.receipt.source_id, prior_source);
    let bindings = vec![HistoricalCatchupSource {
        source_id: prior_source.clone(),
        commit_id,
        receipt_sha256: sha256(&serde_json::to_vec(&canonical).unwrap()),
        head,
    }];
    let mut input = s1_plan(&rows);
    let external = if refresh {
        "hc-regrouped-original"
    } else {
        "hc-regrouped-third"
    };
    input.snapshot_sha256 = if refresh { "6" } else { "a" }.repeat(64);
    input.plan_id = if refresh { "7" } else { "c" }.repeat(64);
    input.groups[0].external_source_id = external.into();
    for document in &mut input.groups[0].documents {
        document.external_source_id = external.into();
    }
    input.documents_sha256 = jsonl_sha256(input.groups[0].documents.iter()).unwrap();
    input.groups_sha256 = jsonl_sha256([
        serde_json::json!({"external_source_id":input.groups[0].external_source_id,"documents":1,
        "document_mapping_sha256":input.groups[0].document_mapping_sha256}),
    ])
    .unwrap();
    let plan = HistoricalRegenerationPlan::derive(&input, &bindings).unwrap();
    let epoch = CanonicalRepository::new(storage.clone())
        .observe_epoch("history-test")
        .await
        .unwrap()
        .unwrap();
    let (report, ledger, report_sha, ledger_sha, native_sha) = baseline(storage, root).await;
    let mut delta = batch(&epoch);
    delta.migration_id = if refresh { "8" } else { "d" }.repeat(64);
    delta.before_snapshot_sha256 = if refresh { "e" } else { "6" }.repeat(64);
    delta.after_snapshot_sha256 = input.snapshot_sha256.clone();
    delta.planner_delta_sha256 = if refresh { "9" } else { "f" }.repeat(64);
    delta.retained_controls[0].before.as_mut().unwrap()["applied_at"] = if refresh {
        "2026-01-02T00:00:00Z"
    } else {
        "2026-01-03T00:00:00Z"
    }
    .into();
    delta.retained_controls[0].after.as_mut().unwrap()["applied_at"] = if refresh {
        "2026-01-03T00:00:00Z"
    } else {
        "2026-01-04T00:00:00Z"
    }
    .into();
    delta.retained_controls[0].before_fingerprint = Some(
        retained_control_fingerprint(delta.retained_controls[0].before.as_ref().unwrap()).unwrap(),
    );
    delta.retained_controls[0].after_fingerprint = Some(
        retained_control_fingerprint(delta.retained_controls[0].after.as_ref().unwrap()).unwrap(),
    );
    let declaration = HistoricalCatchupInput {
        version: 1,
        journal_identity: storage.erasure_journal().unwrap().identity().into(),
        migration_id: delta.migration_id.clone(),
        source_space_id: "default".into(),
        space_id: "history-test".into(),
        before_snapshot_sha256: delta.before_snapshot_sha256.clone(),
        after_snapshot_sha256: delta.after_snapshot_sha256.clone(),
        delta_sha256: delta.planner_delta_sha256.clone(),
        original_report_sha256: report_sha,
        baseline_ledger_sha256: ledger_sha,
        baseline_native_rows_sha256: native_sha,
        accept_deletions: false,
        expected_epoch: epoch,
        batches: 1,
        regeneration_plan_sha256: plan.plan_sha256.clone(),
        regeneration_steps: plan.steps.len() as u64,
        sources: bindings.clone(),
    };
    let retired_commit = crate::source_publication::stable_key(&[
        "historical-s1-retirement-v1",
        &plan.plan_sha256,
        &prior_source.to_string(),
    ]);
    let ledger_copy = root.join("third-ledger.sqlite");
    storage
        .with_historical_catchup(declaration, move |mut custody| async move {
            let index = PreparedHistoricalEffectIndex::prepare(&custody, &bindings).await?;
            let mut expected =
                HistoricalCatchupLedger::open(&custody, &report, &ledger, &ledger_copy)?;
            let (_, effects) = custody
                .apply_batch_with_effects(&index, delta.prepare()?)
                .await?;
            expected.apply(effects.unwrap())?;
            let sealed = custody.seal_for_regeneration().await?;
            let client = sealed.client()?.clone();
            let prepared = HistoricalAdoptionRepository::new(client)
                .prepare_catchup_authority(input, sealed)
                .await?;
            prepared.adopt_group(external, rows).await?;
            expected.apply(prepared.recovered_regeneration_effects(0).await?)?;
            if !refresh {
                prepared.retire_next_group_with_effects().await?;
                expected.apply(prepared.recovered_regeneration_effects(1).await?)?;
            }
            expected
                .complete(prepared.close_regeneration().await?)
                .await
        })
        .await
        .unwrap();
    retired_commit
}
