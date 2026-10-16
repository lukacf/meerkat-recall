//! Actual custody-owned snapshot anchors preserve bytes and separate import
//! evidence from authorship. No provider or extraction work is executed.
use super::*;
use crate::historical_adoption::{HistoricalSnapshotGraphGroup, HistoricalSnapshotGraphRow};
use crate::read_eligibility::ReadEligibility;
use crate::read_eligibility::{CurrentRead, ReadAuthorization};
use std::future::Future;
use std::pin::Pin;

// Construct the large generic read future in a non-inlined frame, then retain
// only its heap handle in the composed fixture. Bounds and read guards stay in
// the production reader; this is not a larger stack or a new execution lane.
#[inline(never)]
fn project<'a, T: CurrentRead + 'a>(
    reader: &'a ReadEligibility,
    row: T,
    authorize: &'a ReadAuthorization<'a>,
) -> Pin<Box<dyn Future<Output = crate::StorageResult<Option<T>>> + Send + 'a>> {
    Box::pin(reader.project(row, authorize))
}

#[allow(
    clippy::too_many_lines,
    reason = "Keep the complete ordered fixture and its failure/recovery assertions together."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve original transfer, read and action owners through snapshot adoption, authorization races and final revocation assertions, including the final client move."
)]
async fn exercise_snapshot(embedded: bool, owner_action: bool) {
    let directory = tempfile::tempdir().unwrap();
    let storage = StorageClient::new_with_durability(
        StorageConfig {
            backend: if embedded {
                crate::client::StorageBackendKind::ElephantDb
            } else {
                crate::client::StorageBackendKind::SurrealDb
            },
            url: "mem://".into(),
            database: "snapshot_anchor_fixture".into(),
            ..StorageConfig::default()
        },
        None,
        Some(directory.path().join("journal")),
    )
    .await
    .unwrap();
    let snapshot = "b".repeat(64);
    let result=storage.with_transfer_barrier(&snapshot.clone(), move |exclusive| async move {
        if !embedded {
            for sql in [
                include_str!("../../migrations/sql/031_source_publication.surql"),
                include_str!("../../migrations/sql/033_space_processing.surql"),
                include_str!("../../migrations/sql/034_storage_durability.surql"),
                include_str!("../../migrations/sql/035_source_publication_manifest.surql"),
                include_str!("../../migrations/sql/037_source_publication_manifest_history.surql"),
                include_str!("../../migrations/sql/038_source_index_visibility.surql"),
                include_str!("../../migrations/sql/039_private_reference_fence.surql"),
                include_str!("../../migrations/sql/043_native_read_fence.surql"),
                include_str!("../../migrations/sql/044_source_head_semantic_epoch.surql"),
                include_str!("../../migrations/sql/045_canonical_read_semantic_epoch.surql"),
                include_str!("../../migrations/sql/046_source_support_index_fence.surql"),
            ] { exclusive.query_checked(sql).await?; }
        }
        exclusive.initialize_journal_identity().await?;
        HistoricalAdoptionRepository::new(exclusive.clone()).bootstrap_configuration(&snapshot, config()).await?;
        let entity=Entity::new(BaseFields::new(RecordId::new_v4(TableName::Entity),"history-test".into(),"original-agent".into()),EntityType::new("person"),"Original subject".into());
        let independent=Entity::new(BaseFields::new(RecordId::new_v4(TableName::Entity),"history-test".into(),"original-owner".into()),EntityType::new("person"),"Independent reference".into());
        let mut base=BaseFields::new(RecordId::new_v4(TableName::Assertion),"history-test".into(),"original-agent".into());
        base.provenance.work_run_id=Some(RecordId::new_v4(TableName::WorkRun));
        base.subjects=vec![Link::new(entity.base.id.clone())];
        let mut assertion_json=serde_json::to_value(&base).unwrap();
        assertion_json.as_object_mut().unwrap().extend(serde_json::json!({
            "subject_text":"Original subject","predicate_raw":if owner_action {"requires_action"} else {"remembered"},"object_text":"Original statement",
            "evidence":{"revision":RecordId::new_v4(TableName::DocRevision),"start":0,"end":0,"quote_hash":""},
            "subject_entity":entity.base.id,"object_entity":independent.base.id,"asserted_at":Utc::now(),"observed_at":Utc::now(),"assertion_key":"agent-original"
        }).as_object().unwrap().clone());
        let assertion:elephant_types::records::Assertion=serde_json::from_value(assertion_json).unwrap();
        let evidence=elephant_types::records::rel::EvidenceRef {assertion:Link::new(assertion.base.id.clone()),span:assertion.evidence.clone(),time_basis:elephant_types::records::rel::TimeBasis::ObservedAt,time_value:assertion.observed_at};
        let attribute=elephant_types::records::Attribute {base:BaseFields::new(RecordId::new_v4(TableName::Attribute),"history-test".into(),"agent".into()),subject:Link::new(entity.base.id.clone()),attr_type:"remembered".into(),value:elephant_types::records::attribute::Literal::String("Original statement".into()),status:elephant_types::records::rel::ClaimStatus::default(),confidence:0.5,validity_window:None,evidence:vec![evidence.clone()],temporal_audit:None};
        let rel=elephant_types::records::Rel {base:BaseFields::new(RecordId::new_v4(TableName::Rel),"history-test".into(),"agent".into()),subject:Link::new(entity.base.id.clone()),object:Link::new(independent.base.id.clone()),rel_type:"remembered".into(),status:elephant_types::records::rel::ClaimStatus::default(),confidence:0.5,validity_window:None,evidence:vec![evidence.clone()],temporal_audit:None};
        let event=elephant_types::records::Event::new(BaseFields::new(RecordId::new_v4(TableName::Event),"history-test".into(),"agent".into()),"Original event",vec![Link::new(assertion.base.id.clone())],vec![]).unwrap();
        let mut truth_json=serde_json::to_value(BaseFields::new(RecordId::new_v4(TableName::TruthSlot),"history-test".into(),"agent".into())).unwrap();
        truth_json.as_object_mut().unwrap().extend(serde_json::json!({"slot_key":"original-agent-slot","slot_type":"event","subject_entity":entity.base.id.to_string(),"predicate_id":"remembered",
            "current_values":[{"claim_id":event.base.id.to_string(),"confidence":0.5,"status":"approved","evidence":[evidence]}],"computed_at":Utc::now(),"security_hash":"retained"}).as_object().unwrap().clone());
        let truth:elephant_types::records::TruthSlot=serde_json::from_value(truth_json).unwrap();
        // Authorization envelopes must agree within a group; the references are
        // separately authorized regardless of the root envelope.
        let mut assertion=assertion; assertion.base.subjects.clear();
        crate::EntityRepository::new(exclusive.clone()).create(entity.clone()).await?;
        crate::EntityRepository::new(exclusive.clone()).create(independent.clone()).await?;
        crate::AssertionRepository::new(exclusive.clone()).create(assertion.clone()).await?;
        crate::EventRepository::new(exclusive.clone()).create(event.clone()).await?;
        crate::TruthSlotRepository::new(exclusive.clone()).create(truth.clone()).await?;
        crate::AttributeRepository::new(exclusive.clone()).create(attribute.clone()).await?;
        crate::RelRepository::new(exclusive.clone()).create(rel.clone()).await?;
        let records=vec![CanonicalRecord::Entity(Box::new(entity.clone())),CanonicalRecord::Assertion(Box::new(assertion.clone())),CanonicalRecord::Event(Box::new(event.clone())),CanonicalRecord::TruthSlot(Box::new(truth.clone())),CanonicalRecord::Attribute(Box::new(attribute.clone())),CanonicalRecord::Rel(Box::new(rel.clone()))];
        let rows:Vec<_>=records.iter().map(|record| {
            let mut original=serde_json::to_value(record).unwrap()["record"].clone(); original["space_id"]="default".into();
            HistoricalSnapshotGraphRow {record:record.clone(),original_row_sha256:canonical_sha256(&original).unwrap(),original_row:original}
        }).collect();
        let group=HistoricalSnapshotGraphGroup::from_rows(rows.iter().map(|row|(row.record.base().id.clone(),row.original_row_sha256.clone())).collect()).unwrap();
        let input=HistoricalAdoptionInput {source_space_id:"default".into(),space_id:"history-test".into(),snapshot_sha256:snapshot,
            idmap_sha256:"c".repeat(64),documents_sha256:sha256(b""),groups_sha256:sha256(b""),plan_id:"d".repeat(64),groups:vec![],snapshot_groups:vec![group]};
        let authority=HistoricalAdoptionRepository::new(exclusive.clone()).prepare_authority(input.clone())?;
        let before=CanonicalRepository::new(exclusive.clone()).observe_epoch("history-test").await?;
        let mut omitted=rows.clone();omitted.pop();assert!(Box::pin(authority.adopt_snapshot_graph(omitted)).await.is_err());
        let mut altered=rows.clone();altered[0].original_row_sha256="0".repeat(64);assert!(Box::pin(authority.adopt_snapshot_graph(altered)).await.is_err());
        assert_eq!(before,CanonicalRepository::new(exclusive.clone()).observe_epoch("history-test").await?);
        assert!(project(&ReadEligibility::new(exclusive.clone()),assertion.clone(),&|_|true).await?.is_none());
        let committed=Box::pin(authority.adopt_snapshot_graph(rows.clone())).await?;
        Box::pin(assert_snapshot_input_domain(&exclusive, &committed.receipt)).await?;
        assert_eq!(committed.receipt.operation.state,OperationState::Processing);
        let replay=Box::pin(authority.adopt_snapshot_graph(rows.clone())).await?;assert_eq!(replay.receipt,committed.receipt);
        for record in &records {
            assert_eq!(CanonicalRecord::load(&exclusive,"history-test",&record.base().id).await?.unwrap().fingerprint()?,record.fingerprint()?);
            let support=SourcePublicationRepository::new(exclusive.clone()).support_for_record("history-test",&record.base().id).await?;
            assert_eq!(support.len(),1);assert!(!support[0].independent);assert!(support[0].historical_unknown.is_some());
        }
        Ok((entity,assertion,event,truth,committed.receipt,input,rows,independent,attribute,rel))
    }).await.unwrap();
    let (entity, assertion, event, truth, receipt, input, rows, mut independent, attribute, rel) =
        result;
    if !embedded {
        refresh_snapshot(&storage, input, rows, &receipt, directory.path()).await;
    }
    let reader = ReadEligibility::new(storage.clone());
    assert!(project(&reader, entity.clone(), &|_| true)
        .await
        .unwrap()
        .is_some());
    assert!(project(&reader, assertion.clone(), &|_| true)
        .await
        .unwrap()
        .is_some());
    assert!(project(&reader, event.clone(), &|_| true)
        .await
        .unwrap()
        .is_some());
    assert!(project(&reader, truth.clone(), &|_| true)
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        project(&reader, attribute.clone(), &|_| true)
            .await
            .unwrap(),
        Some(attribute.clone())
    );
    assert_eq!(
        project(&reader, rel.clone(), &|_| true).await.unwrap(),
        Some(rel.clone())
    );
    assert_eq!(
        project(&reader, truth.clone(), &|_| true)
            .await
            .unwrap()
            .unwrap()
            .current_values[0]
            .evidence,
        truth.current_values[0].evidence
    );
    assert!(reader
        .evidence("history-test", &assertion.evidence, &|_| true)
        .await
        .unwrap()
        .is_none());
    assert_eq!(assertion.base.provenance.sources.len(), 0);
    assert!(assertion.base.provenance.work_run_id.is_some());
    for denied in [&entity.base.id, &independent.base.id, &receipt.source_id] {
        let reader = ReadEligibility::new(storage.clone());
        assert!(
            project(&reader, attribute.clone(), &|base| &base.id != denied)
                .await
                .unwrap()
                .is_none()
        );
        assert!(project(&reader, rel.clone(), &|base| &base.id != denied)
            .await
            .unwrap()
            .is_none());
        assert!(
            project(&reader, assertion.clone(), &|base| &base.id != denied)
                .await
                .unwrap()
                .is_none()
        );
        assert!(project(&reader, event.clone(), &|base| &base.id != denied)
            .await
            .unwrap()
            .is_none());
        assert!(project(&reader, truth.clone(), &|base| &base.id != denied)
            .await
            .unwrap()
            .is_none());
    }
    if owner_action {
        use crate::action_disposition::{ActionDispositionRepository, CloseActionInput};
        use crate::read_eligibility::CurrentRead;
        use elephant_types::records::doc_revision::{
            DocRevision, RendererMetadata, RevisionStatus,
        };
        let doc = elephant_types::records::Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "history-test".into(),
                "current-owner".into(),
            ),
            title: "New explicit owner statement".into(),
            text: "I close this exact request without asserting that it was performed.".into(),
            sidecar_text: None,
            source: None,
            media_attachments: vec![],
            rendition_key: "owner-new".into(),
            metadata: serde_json::json!({"recorded_by":"not-authority"}),
        };
        crate::DocRepository::new(storage.clone())
            .create(doc.clone())
            .await
            .unwrap();
        let revision = DocRevision {
            base: BaseFields::new(
                RecordId::new_v4(TableName::DocRevision),
                "history-test".into(),
                "current-renderer".into(),
            ),
            doc: Link::new(doc.base.id.clone()),
            text: doc.text.clone(),
            text_hash: sha256(doc.text.as_bytes()),
            renderer: RendererMetadata {
                name: "current".into(),
                version: "1".into(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: Utc::now(),
        };
        crate::DocRevisionRepository::new(storage.clone())
            .create(revision.clone())
            .await
            .unwrap();
        let input = CloseActionInput {
            action_id: assertion.base.id.clone(),
            subject_entity: entity.base.id.clone(),
            owner_statement: elephant_types::records::assertion::EvidenceSpan {
                revision: Link::new(revision.base.id),
                node: None,
                start: 0,
                end: u32::try_from(doc.text.len()).unwrap(),
                quote_hash: revision.text_hash,
            },
            reason: "Closed by the owner; original request retained".into(),
            effective_at: Utc::now(),
        };
        let actions = ActionDispositionRepository::new(storage.clone());
        let closed = Box::pin(actions.close(
            "history-test",
            actions.begin("history-test").await.unwrap(),
            "authenticated-current-owner",
            input.clone(),
            &|_| true,
        ))
        .await
        .unwrap();
        let replay = Box::pin(actions.close(
            "history-test",
            actions.begin("history-test").await.unwrap(),
            "authenticated-current-owner",
            input,
            &|_| true,
        ))
        .await
        .unwrap();
        assert_eq!(closed, replay);
        assert_eq!(
            crate::AssertionRepository::new(storage.clone())
                .get("history-test", &assertion.base.id)
                .await
                .unwrap(),
            Some(assertion.clone())
        );
        assert_eq!(
            crate::EventRepository::new(storage.clone())
                .get("history-test", &event.base.id)
                .await
                .unwrap(),
            Some(event.clone())
        );
        let reader = ReadEligibility::new(storage.clone());
        let mut projection = project(&reader, event.clone(), &|_| true)
            .await
            .unwrap()
            .unwrap();
        projection
            .project_action_state(&reader, &|_| true)
            .await
            .unwrap();
        assert_eq!(
            projection.metadata["recall_action_dispositions"][0]["state"],
            "closed_by_owner"
        );
        assert!(actions
            .current("history-test", &assertion.base.id, &|base| base.id
                != receipt.source_id)
            .await
            .unwrap()
            .is_none());
        assert!(assertion.base.provenance.work_run_id.is_some());
    }
    // Deterministic response assembly race: this referenced Entity has no
    // support or source head. A legitimate edit after projection must invalidate
    // the shared response fence rather than return a previously authorized image.
    assert!(SourcePublicationRepository::new(storage.clone())
        .support_for_record("history-test", &independent.base.id)
        .await
        .unwrap()
        .is_empty());
    let fenced = crate::read_eligibility::with_read_fence(storage.clone(), async {
        assert!(project(
            &ReadEligibility::new(storage.clone()),
            assertion.clone(),
            &|_| true
        )
        .await
        .unwrap()
        .is_some());
        independent.display_name = "Changed after authorization".into();
        crate::EntityRepository::new(storage.clone())
            .update("history-test", &independent.base.id.clone(), independent)
            .await
            .unwrap();
    })
    .await;
    assert!(matches!(fenced, Err(crate::StorageError::Conflict(_))));
    // Simulate storage corruption only in the fixture: retained reference loses
    // its original namespace. The parent certificate cannot override this.
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| {
                tx.hard_delete::<Entity>("history-test", &entity.base.id)?;
                Ok(())
            })
            .await
            .unwrap();
    } else {
        storage
            .query_bind_checked(
                "UPDATE ONLY $id SET space_id='foreign'",
                crate::bindings! {"id"=>crate::surreal::thing_from_record_id(&entity.base.id)},
            )
            .await
            .unwrap();
    }
    assert!(
        project(&ReadEligibility::new(storage.clone()), assertion, &|_| true)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        project(&ReadEligibility::new(storage.clone()), event, &|_| true)
            .await
            .unwrap()
            .is_none()
    );
    assert!(ReadEligibility::new(storage)
        .project(truth, &|_| true)
        .await
        .unwrap()
        .is_none());
}
#[tokio::test]
async fn historical_snapshot_anchor_embedded_exact_rows_and_reference_authorization() {
    Box::pin(exercise_snapshot(true, false)).await;
}
#[tokio::test]
async fn historical_snapshot_anchor_native_exact_rows_and_reference_authorization() {
    Box::pin(exercise_snapshot(false, false)).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the publication reader through exact source/operation/revision/commit identity comparisons under caller-owned storage custody."
)]
async fn assert_snapshot_input_domain(
    storage: &StorageClient,
    receipt: &HistoricalAdoptionReceipt,
) -> crate::StorageResult<()> {
    let publications = SourcePublicationRepository::new(storage.clone());
    let operation: crate::source_publication::StoredOperation = publications
        .read_control(
            "source_operation",
            &receipt.operation.operation_id,
            &receipt.space_id,
        )
        .await?
        .ok_or_else(|| integrity("snapshot operation fixture missing"))?;
    let input = uuid::Uuid::parse_str(&operation.input_fingerprint)
        .map_err(|_| integrity("snapshot operation input must be a UUID"))?;
    assert_eq!(input.get_version_num(), 5);
    assert_eq!(input.to_string(), operation.input_fingerprint);
    assert_eq!(
        operation.request_fingerprint,
        receipt.accepted_request_fingerprint
    );
    assert_eq!(receipt.accepted_request_fingerprint.len(), 64);
    assert_ne!(
        operation.input_fingerprint,
        receipt.accepted_request_fingerprint
    );
    let revision: crate::source_publication::StoredRevision = publications
        .read_control(
            "source_revision",
            &crate::source_publication::revision_key(
                &receipt.space_id,
                &receipt.source_id,
                &receipt.accepted_revision,
            ),
            &receipt.space_id,
        )
        .await?
        .ok_or_else(|| integrity("snapshot revision fixture missing"))?;
    assert_eq!(revision.input_fingerprint, operation.input_fingerprint);
    let canonical = CanonicalRepository::new(storage.clone())
        .receipt_read_only(&receipt.space_id, &receipt.commit_id)
        .await?
        .ok_or_else(|| integrity("snapshot canonical receipt fixture missing"))?;
    assert_eq!(
        canonical.input_fingerprint,
        receipt.accepted_request_fingerprint
    );
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "Keep the complete ordered fixture and its failure/recovery assertions together."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve original regeneration authority consumption and publication reader through final ledger closure and original source timestamp readback."
)]
async fn refresh_snapshot(
    storage: &StorageClient,
    mut input: HistoricalAdoptionInput,
    rows: Vec<HistoricalSnapshotGraphRow>,
    receipt: &HistoricalAdoptionReceipt,
    root: &std::path::Path,
) {
    use crate::client::{HistoricalCatchupInput, HistoricalCatchupSource};
    use crate::historical_catchup_ledger::HistoricalCatchupLedger;
    use crate::historical_delta::{
        retained_control_fingerprint, HistoricalDeltaBatch, HistoricalRetainedControlDelta,
    };
    let head = SourcePublicationRepository::new(storage.clone())
        .head("history-test", &receipt.source_id)
        .await
        .unwrap()
        .unwrap();
    let canonical = CanonicalRepository::new(storage.clone())
        .receipt_read_only("history-test", &receipt.commit_id)
        .await
        .unwrap()
        .unwrap();
    let old_revision_key = crate::source_publication::revision_key(
        "history-test",
        &receipt.source_id,
        &receipt.accepted_revision,
    );
    let old_revision = super::super::catchup::full_control(
        storage,
        "source_revision",
        &old_revision_key,
        "history-test",
    )
    .await
    .unwrap();
    let old_support = SourcePublicationRepository::new(storage.clone())
        .support_for_record("history-test", &rows[0].record.base().id)
        .await
        .unwrap();
    let original_record = rows[0].record.base().id.clone();
    let original_source = crate::SourceRepository::new(storage.clone())
        .get("history-test", &receipt.source_id)
        .await
        .unwrap()
        .unwrap();
    let bindings = vec![HistoricalCatchupSource {
        source_id: receipt.source_id.clone(),
        commit_id: receipt.commit_id.clone(),
        receipt_sha256: sha256(&serde_json::to_vec(&canonical).unwrap()),
        head,
    }];
    input.snapshot_sha256 = "e".repeat(64);
    input.plan_id = "f".repeat(64);
    let plan = HistoricalRegenerationPlan::derive(&input, &bindings).unwrap();
    assert_eq!(plan.steps.len(), 1);
    let epoch = CanonicalRepository::new(storage.clone())
        .observe_epoch("history-test")
        .await
        .unwrap()
        .unwrap();
    let ledger_root = root.join("snapshot-refresh");
    std::fs::create_dir(&ledger_root).unwrap();
    let (report, ledger, report_sha, ledger_sha, native_sha) =
        super::s1_tests::baseline(storage, &ledger_root).await;
    let control = serde_json::json!({"id":"migration_history:fake_embedding_purge_v1","name":crate::fake_embedding_repair::RECEIPT_NAME,
        "checksum":crate::fake_embedding_repair::RECEIPT_CHECKSUM,"applied_at":"2026-01-01T00:00:00Z"});
    let delta = HistoricalDeltaBatch {
        version: 1,
        migration_id: "1".repeat(64),
        source_space_id: "default".into(),
        space_id: "history-test".into(),
        before_snapshot_sha256: "b".repeat(64),
        after_snapshot_sha256: "e".repeat(64),
        planner_delta_sha256: "2".repeat(64),
        batch_ordinal: 0,
        previous_batch_receipt: None,
        expected_epoch: epoch.clone(),
        rows: vec![],
        embeddings: vec![],
        retained_controls: vec![HistoricalRetainedControlDelta {
            before: None,
            before_fingerprint: None,
            after_fingerprint: Some(retained_control_fingerprint(&control).unwrap()),
            after: Some(control),
        }],
    };
    let custody = HistoricalCatchupInput {
        version: 1,
        journal_identity: storage.erasure_journal().unwrap().identity().into(),
        migration_id: delta.migration_id.clone(),
        source_space_id: "default".into(),
        space_id: "history-test".into(),
        before_snapshot_sha256: "b".repeat(64),
        after_snapshot_sha256: "e".repeat(64),
        delta_sha256: "2".repeat(64),
        original_report_sha256: report_sha,
        baseline_ledger_sha256: ledger_sha,
        baseline_native_rows_sha256: native_sha,
        accept_deletions: false,
        expected_epoch: epoch,
        batches: 1,
        regeneration_plan_sha256: plan.plan_sha256,
        regeneration_steps: 1,
        sources: bindings.clone(),
    };
    let copy = ledger_root.join("work.sqlite");
    storage
        .with_historical_catchup(custody, move |mut custody| async move {
            let index = PreparedHistoricalEffectIndex::prepare(&custody, &bindings).await?;
            let mut ledger = HistoricalCatchupLedger::open(&custody, &report, &ledger, &copy)?;
            let (_, effects) = custody
                .apply_batch_with_effects(&index, delta.prepare()?)
                .await?;
            ledger.apply(effects.unwrap())?;
            let sealed = custody.seal_for_regeneration().await?;
            let client = sealed.client()?.clone();
            let authority = HistoricalAdoptionRepository::new(client.clone())
                .prepare_catchup_authority(input, sealed)
                .await?;
            let committed = Box::pin(authority.adopt_snapshot_graph(rows)).await?;
            Box::pin(assert_snapshot_input_domain(&client, &committed.receipt)).await?;
            ledger.apply(authority.recovered_regeneration_effects(0).await?)?;
            ledger.complete(authority.close_regeneration().await?).await
        })
        .await
        .unwrap();
    let publications = SourcePublicationRepository::new(storage.clone());
    let current = publications
        .head("history-test", &receipt.source_id)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        current.published_revision.as_deref(),
        Some(receipt.accepted_revision.as_str())
    );
    assert_eq!(
        super::super::catchup::full_control(
            storage,
            "source_revision",
            &old_revision_key,
            "history-test"
        )
        .await
        .unwrap(),
        old_revision,
        "original immutable native revision is retained exactly",
    );
    let support = publications
        .support_for_record("history-test", &original_record)
        .await
        .unwrap();
    assert_eq!(support.len(), old_support.len() + 1);
    for old in old_support {
        assert!(support.contains(&old), "original support remains intact");
    }
    let source = crate::SourceRepository::new(storage.clone())
        .get("history-test", &receipt.source_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source.base.created_at, original_source.base.created_at);
}

#[tokio::test]
async fn historical_snapshot_owner_action_after_real_import_both_backends() {
    for embedded in [true, false] {
        Box::pin(exercise_snapshot(embedded, true)).await;
    }
}
