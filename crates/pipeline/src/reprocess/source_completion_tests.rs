//! Real retained completion preparation and repository rollback oracles. No model calls.
use super::*;
use elephant_storage::canonical_commit::{CanonicalEffect, CanonicalRecord, CanonicalRepository};
use elephant_storage::{AttributeRepository, EntityRepository, TruthSlotRepository};
use elephant_types::records::{
    Attribute, ClaimStatus, Entity, EntityType, Literal, SubjectsState, TruthSlot, WorkItemStatus,
};

fn classified(table: TableName) -> BaseFields {
    let mut row = base(table);
    row.subjects_state = SubjectsState::Complete;
    row
}

async fn slot(
    ctx: &ReprocessExecutionContext,
    predicate: &str,
    values: &[i64],
) -> (TruthSlot, Vec<Attribute>) {
    let subject = EntityRepository::new(ctx.storage.clone())
        .create(Entity::new(
            classified(TableName::Entity),
            EntityType::new("person"),
            predicate.into(),
        ))
        .await
        .unwrap();
    let mut claims = Vec::new();
    for value in values {
        claims.push(
            AttributeRepository::new(ctx.storage.clone())
                .create(Attribute {
                    base: classified(TableName::Attribute),
                    subject: Link::new(subject.base.id.clone()),
                    attr_type: predicate.into(),
                    value: Literal::Integer(*value),
                    status: ClaimStatus::Candidate,
                    confidence: 0.8,
                    validity_window: None,
                    evidence: Vec::new(),
                    temporal_audit: None,
                })
                .await
                .unwrap(),
        );
    }
    let security_hash = crate::truth_maintenance::compute_record_partition(&claims[0].base);
    let slot = TruthSlotRepository::new(ctx.storage.clone())
        .create(TruthSlot {
            base: classified(TableName::TruthSlot),
            slot_key: crate::truth_maintenance::compute_slot_key(
                &subject.base.id.to_string(),
                None,
                predicate,
                None,
                &security_hash,
            ),
            slot_type: "attribute".into(),
            subject_entity: subject.base.id.to_string(),
            predicate_id: predicate.into(),
            current_values: Vec::new(),
            conflict_group: None,
            confidence: 0.0,
            rationale: None,
            semantics_version: 0,
            alias_version: 0,
            stale: false,
            stale_reason: None,
            computed_at: Utc::now() - chrono::Duration::days(10),
            security_hash,
        })
        .await
        .unwrap();
    (slot, claims)
}

async fn all_stages_job(ctx: &ReprocessExecutionContext) -> ReprocessJob {
    create_reprocess_job(
        &ctx.job_repo,
        "Original all-stage completion",
        SelectionCriteria {
            source_types: vec!["email".into()],
            ..Default::default()
        },
        elephant_types::records::reprocess::default_reprocess_target_stages(),
        SPACE,
        "oracle",
        None,
    )
    .await
    .unwrap()
    .job
}

#[tokio::test]
async fn all_stages_empty_selection_recomputes_and_retains_real_pending_conflicts() {
    for embedded in [true, false] {
        let (ctx, _) = Box::pin(setup(embedded)).await;
        let (single, _) = Box::pin(slot(&ctx, "single", &[35])).await;
        let (divergent, _) = Box::pin(slot(&ctx, "divergent", &[35, 36])).await;
        let original = Box::pin(all_stages_job(&ctx)).await;
        let (_, run, attempt) = Box::pin(claimed_coordinator_run(&ctx, &original)).await;
        let config = ReprocessConfig::default();
        let first = Box::pin(execute_reprocess_job_with_attempt(
            &ctx,
            &original.base.id,
            &run.base.id,
            SPACE,
            &config,
            Some(attempt.clone()),
        ))
        .await
        .unwrap();
        assert!(first.continuation_required);
        assert!(first.pending_operations.is_empty());
        assert_eq!(first.sources_processed, 0);
        assert_eq!(first.pending_conflict_groups.len(), 1);
        let group = elephant_storage::ConflictGroupRepository::new(ctx.storage.clone())
            .find_unresolved_by_slot_key_and_type(SPACE, &divergent.slot_key, "attribute")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.pending_conflict_groups, vec![group.base.id.clone()]);
        assert_eq!(group.claim_ids.len(), 2);
        let current = ctx
            .job_repo
            .get(SPACE, &original.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.status, ReprocessStatus::Paused);
        assert!(current.completed_at.is_none());
        let slots = TruthSlotRepository::new(ctx.storage.clone());
        let clean = slots.get(SPACE, &single.base.id).await.unwrap().unwrap();
        assert!(!clean.stale);
        assert!(!clean.current_values.is_empty());
        assert!(clean.computed_at > single.computed_at);
        assert!(
            slots
                .get(SPACE, &divergent.base.id)
                .await
                .unwrap()
                .unwrap()
                .stale
        );
        let artifacts = WorkArtifactRepository::new(ctx.storage.clone());
        let seed = artifacts
            .get(SPACE, &maintenance_id(&original.base.id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(seed.work_run.id(), &run.base.id);
        let second = Box::pin(execute_reprocess_job_with_attempt(
            &ctx,
            &original.base.id,
            &run.base.id,
            SPACE,
            &config,
            Some(attempt),
        ))
        .await
        .unwrap();
        assert!(second.continuation_required);
        assert_eq!(
            second.pending_conflict_groups,
            first.pending_conflict_groups
        );
        assert_eq!(
            slots.get(SPACE, &single.base.id).await.unwrap(),
            Some(clean)
        );
        assert_eq!(
            artifacts.get(SPACE, &seed.base.id).await.unwrap(),
            Some(seed.clone())
        );
        // A changed continuation payload cannot reuse the original seed.
        let planning = Box::pin(PlanningTruthMaintenanceStorage::new(
            ctx.storage.clone(),
            SPACE,
            None,
        ))
        .await
        .unwrap();
        let current = ctx
            .job_repo
            .get(SPACE, &original.base.id)
            .await
            .unwrap()
            .unwrap();
        let error = Box::pin(prepare_completed_job(
            &ctx,
            &planning,
            current.clone(),
            &run.base.id,
            &serde_json::json!({"operations":["changed-original-operation"]}),
            0,
            &mut ReprocessResult::default(),
        ))
        .await
        .unwrap_err();
        assert!(error.to_string().contains("maintenance intent changed"));
        assert_eq!(
            ctx.job_repo.get(SPACE, &original.base.id).await.unwrap(),
            Some(current)
        );
        assert_eq!(
            artifacts.get(SPACE, &seed.base.id).await.unwrap(),
            Some(seed)
        );
    }
}

/// A real source-less document/assertion lineage under the existing repository
/// contract. No accepted Source, source head, or publication certificate is made.
async fn retain_original_claim_evidence(
    ctx: &ReprocessExecutionContext,
    claim: &Attribute,
) -> Attribute {
    use elephant_types::records::assertion::{Assertion, EvidenceSpan};
    use elephant_types::records::doc_revision::{RendererMetadata, RevisionStatus};
    use elephant_types::records::{Doc, EvidenceRef, TimeBasis};
    let text = "The recorded age is 35.";
    let doc = ctx
        .doc_repo
        .create(Doc {
            base: classified(TableName::Doc),
            title: "Original age record".into(),
            text: text.into(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "original-source-less-age".into(),
            metadata: serde_json::json!({}),
        })
        .await
        .unwrap();
    let hash = crate::extraction::AssertionValidator::hash_quote(text);
    let revision = ctx
        .doc_revision_repo
        .create(DocRevision {
            base: classified(TableName::DocRevision),
            doc: Link::new(doc.base.id.clone()),
            text: text.into(),
            text_hash: hash.clone(),
            renderer: RendererMetadata {
                name: "original-fixture".into(),
                version: "1".into(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: Utc::now(),
        })
        .await
        .unwrap();
    let span = EvidenceSpan {
        revision: Link::new(revision.base.id.clone()),
        node: None,
        start: 0,
        end: u32::try_from(text.len()).unwrap(),
        quote_hash: hash,
    };
    let now = Utc::now();
    let assertion = ctx
        .assertion_repo
        .create(Assertion {
            base: classified(TableName::Assertion),
            subject_text: "age".into(),
            predicate_raw: "age".into(),
            predicate_id: Some("age".into()),
            object_text: None,
            object_literal: Some(serde_json::json!(35)),
            evidence: span.clone(),
            source_event_time: None,
            observed_at: now,
            asserted_at: now,
            assertion_key: format!("original-age:{}", claim.base.id),
            resolution_status: elephant_types::records::assertion::ResolutionStatus::Resolved,
            resolution_method: Some("fixture-original".into()),
            resolution_confidence: Some(0.8),
            retry_state: Default::default(),
            subject_entity: Some(claim.subject.clone()),
            object_entity: None,
            confidence: 0.8,
            subject_entity_type: Some("person".into()),
            object_entity_type: None,
            significance: Some("medium".into()),
            lifecycle_tier: "active".into(),
        })
        .await
        .unwrap();
    let mut supported = claim.clone();
    supported.evidence = vec![EvidenceRef {
        assertion: Link::new(assertion.base.id),
        span: span.clone(),
        time_basis: TimeBasis::ObservedAt,
        time_value: now,
    }];
    let supported = AttributeRepository::new(ctx.storage.clone())
        .update(SPACE, &claim.base.id, supported)
        .await
        .unwrap();
    let reader = elephant_storage::read_eligibility::ReadEligibility::new(ctx.storage.clone());
    let current = Box::pin(reader.evidence(SPACE, &span, &|_| true))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.quote, text);
    assert_eq!(current.revision, revision);
    assert_eq!(current.doc, doc);
    assert!(current.source.is_none());
    assert!(ctx
        .source_repo
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .is_empty());
    supported
}

#[tokio::test]
async fn all_stages_complete_only_after_recompute_and_omitted_maintenance_stays_omitted() {
    for embedded in [true, false] {
        for (requested, with_evidence) in
            [(false, true), (true, true), (false, false), (true, false)]
        {
            let (ctx, run) = Box::pin(setup(embedded)).await;
            let (before, mut claims) = Box::pin(slot(&ctx, "age", &[35])).await;
            if with_evidence {
                claims[0] = Box::pin(retain_original_claim_evidence(&ctx, &claims[0])).await;
            }
            let config = ReprocessConfig::default();
            let original = if requested {
                Box::pin(all_stages_job(&ctx)).await
            } else {
                Box::pin(job(&ctx, &config)).await
            };
            let result = Box::pin(execute_reprocess_job(
                &ctx,
                &original.base.id,
                &run,
                SPACE,
                &config,
            ))
            .await
            .unwrap();
            assert!(!result.continuation_required);
            assert!(result.pending_conflict_groups.is_empty());
            let completed = ctx
                .job_repo
                .get(SPACE, &original.base.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(completed.status, ReprocessStatus::Completed);
            assert!(completed.completed_at.is_some());
            let after = TruthSlotRepository::new(ctx.storage.clone())
                .get(SPACE, &before.base.id)
                .await
                .unwrap()
                .unwrap();
            let claim = AttributeRepository::new(ctx.storage.clone())
                .get(SPACE, &claims[0].base.id)
                .await
                .unwrap()
                .unwrap();
            let seed = WorkArtifactRepository::new(ctx.storage.clone())
                .get(SPACE, &maintenance_id(&original.base.id))
                .await
                .unwrap();
            if requested {
                assert!(!after.stale);
                assert!(after.computed_at > before.computed_at);
                assert!(!after.current_values.is_empty());
                assert_eq!(claim.evidence, claims[0].evidence);
                if with_evidence {
                    assert_eq!(claim.status, ClaimStatus::Approved);
                } else {
                    assert_eq!(claim.status, ClaimStatus::Disputed);
                    assert!(after
                        .rationale
                        .as_deref()
                        .is_some_and(|reason| reason.contains("Promotion thresholds not met")
                            && reason.contains("min_evidence=1")));
                }
                assert!(seed.is_some());
            } else {
                assert_eq!(after, before);
                assert_eq!(claim, claims[0]);
                assert!(seed.is_none());
            }
            let retry = Box::pin(execute_reprocess_job(
                &ctx,
                &original.base.id,
                &run,
                SPACE,
                &config,
            ))
            .await
            .unwrap();
            assert!(!retry.continuation_required);
            assert_eq!(
                TruthSlotRepository::new(ctx.storage.clone())
                    .get(SPACE, &before.base.id)
                    .await
                    .unwrap(),
                Some(after)
            );
        }
    }
}

#[tokio::test]
async fn source_completion_before_image_conflict_preserves_job_seed_claim_slot_and_outbox() {
    for embedded in [true, false] {
        let (ctx, run) = Box::pin(setup(embedded)).await;
        let (before_slot, claims) = Box::pin(slot(&ctx, "age", &[35])).await;
        let original = Box::pin(all_stages_job(&ctx)).await;
        let collision = EntityRepository::new(ctx.storage.clone())
            .create(Entity::new(
                classified(TableName::Entity),
                EntityType::new("topic"),
                "Already present".into(),
            ))
            .await
            .unwrap();
        let events = elephant_storage::SystemEventRepository::new(ctx.storage.clone());
        let before_events = events.list(SPACE, 100, None).await.unwrap();
        let planning = Box::pin(PlanningTruthMaintenanceStorage::new(
            ctx.storage.clone(),
            SPACE,
            None,
        ))
        .await
        .unwrap();
        let request = serde_json::json!({"operations":[]});
        let mut result = ReprocessResult::default();
        Box::pin(prepare_completed_job(
            &ctx,
            &planning,
            original.clone(),
            &run,
            &request,
            0,
            &mut result,
        ))
        .await
        .unwrap();
        assert!(!result.continuation_required);
        let mut prepared = Box::pin(planning.prepare_request(
            "reprocess_completion",
            "source-completion-before-image-conflict",
            &request,
            &result,
        ))
        .await
        .unwrap();
        assert!(prepared.plan.effects.len() >= 4);
        assert!(!prepared.plan.outbox.is_empty());
        // Every original before-image is validated before any effect write.
        // This extra create of an existing row exercises that preflight guard,
        // not the separate CanonicalFault hooks after writes/outbox.
        prepared.plan.effects.push(CanonicalEffect {
            before: None,
            after: Some(CanonicalRecord::Entity(Box::new(collision))),
        });
        let receipt_id = prepared.commit_id.clone();
        let canonical = CanonicalRepository::new(ctx.storage.clone());
        let error = Box::pin(canonical.commit(prepared)).await.unwrap_err();
        let diagnostic = error.to_string();
        assert!(
            diagnostic.contains("canonical_effect_changed")
                || diagnostic.contains("canonical_row_exists"),
            "{diagnostic}"
        );
        assert_eq!(
            ctx.job_repo.get(SPACE, &original.base.id).await.unwrap(),
            Some(original.clone())
        );
        assert_eq!(
            TruthSlotRepository::new(ctx.storage.clone())
                .get(SPACE, &before_slot.base.id)
                .await
                .unwrap(),
            Some(before_slot)
        );
        assert_eq!(
            AttributeRepository::new(ctx.storage.clone())
                .get(SPACE, &claims[0].base.id)
                .await
                .unwrap(),
            Some(claims[0].clone())
        );
        assert!(WorkArtifactRepository::new(ctx.storage.clone())
            .get(SPACE, &maintenance_id(&original.base.id))
            .await
            .unwrap()
            .is_none());
        assert_eq!(events.list(SPACE, 100, None).await.unwrap(), before_events);
        assert!(canonical
            .receipt(SPACE, &receipt_id)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn source_completion_refuses_original_plan_after_configuration_or_attempt_changes() {
    for embedded in [true, false] {
        for change_attempt in [false, true] {
            let (ctx, _) = Box::pin(setup(embedded)).await;
            let (before_slot, claims) = Box::pin(slot(&ctx, "age", &[35])).await;
            let original = Box::pin(all_stages_job(&ctx)).await;
            let (work, run, attempt) = Box::pin(claimed_coordinator_run(&ctx, &original)).await;
            let planning = Box::pin(PlanningTruthMaintenanceStorage::new(
                ctx.storage.clone(),
                SPACE,
                Some(attempt.clone()),
            ))
            .await
            .unwrap();
            let request = serde_json::json!({"operations":[]});
            let mut result = ReprocessResult::default();
            Box::pin(prepare_completed_job(
                &ctx,
                &planning,
                original.clone(),
                &run.base.id,
                &request,
                0,
                &mut result,
            ))
            .await
            .unwrap();
            let prepared = Box::pin(planning.prepare_request(
                "reprocess_completion",
                "original-captured-completion",
                &request,
                &result,
            ))
            .await
            .unwrap();
            let receipt_id = prepared.commit_id.clone();
            if change_attempt {
                elephant_storage::WorkItemRepository::new(ctx.storage.clone())
                    .update_running_for_worker(
                        SPACE,
                        &work.base.id,
                        &attempt.lease_token,
                        WorkItemStatus::Runnable,
                        None,
                    )
                    .await
                    .unwrap()
                    .unwrap();
            } else {
                let repo = SpaceConfigRepository::new(ctx.storage.clone());
                let before = repo.get_by_space_id(SPACE).await.unwrap().unwrap();
                let mut changed = before.clone();
                changed.processing.as_mut().unwrap().generation += 1;
                repo.replace_authorized(&before, changed).await.unwrap();
            }
            let canonical = CanonicalRepository::new(ctx.storage.clone());
            assert!(Box::pin(canonical.commit(prepared)).await.is_err());
            assert_eq!(
                ctx.job_repo.get(SPACE, &original.base.id).await.unwrap(),
                Some(original.clone())
            );
            assert_eq!(
                TruthSlotRepository::new(ctx.storage.clone())
                    .get(SPACE, &before_slot.base.id)
                    .await
                    .unwrap(),
                Some(before_slot)
            );
            assert_eq!(
                AttributeRepository::new(ctx.storage.clone())
                    .get(SPACE, &claims[0].base.id)
                    .await
                    .unwrap(),
                Some(claims[0].clone())
            );
            assert!(WorkArtifactRepository::new(ctx.storage.clone())
                .get(SPACE, &maintenance_id(&original.base.id))
                .await
                .unwrap()
                .is_none());
            assert!(canonical
                .receipt(SPACE, &receipt_id)
                .await
                .unwrap()
                .is_none());
        }
    }
}

#[tokio::test]
async fn retained_completion_artifact_keeps_original_run_and_rejects_changed_certificate() {
    for embedded in [true, false] {
        let (ctx, run) = Box::pin(setup(embedded)).await;
        // Artifact retention is tested independently of source certification:
        // no operation or publication receipt is minted by this control.
        let mut original = artifact(
            base(TableName::WorkArtifact),
            RecordId::new_v4(TableName::WorkArtifact),
            &run,
            "reprocess_source_completion/v1",
            serde_json::json!({"canonical_commit":"original"}),
        );
        original.status = ArtifactStatus::Committed;
        original.committed_at = Some(Utc::now());
        let original = WorkArtifactRepository::new(ctx.storage.clone())
            .create(original)
            .await
            .unwrap();
        let planning = Box::pin(PlanningTruthMaintenanceStorage::new(
            ctx.storage.clone(),
            SPACE,
            None,
        ))
        .await
        .unwrap();
        let mut repeated = original.clone();
        repeated.work_run = Link::new(RecordId::new_v4(TableName::WorkRun));
        repeated.committed_at = Some(Utc::now());
        Box::pin(retain_completion_artifact(
            &ctx,
            &planning,
            SPACE,
            repeated.clone(),
        ))
        .await
        .unwrap();
        assert!(planning.prepared_plan().unwrap().effects.is_empty());
        assert_eq!(
            WorkArtifactRepository::new(ctx.storage.clone())
                .get(SPACE, &original.base.id)
                .await
                .unwrap(),
            Some(original.clone())
        );
        repeated.data["canonical_commit"] = serde_json::json!("replacement");
        let error = Box::pin(retain_completion_artifact(&ctx, &planning, SPACE, repeated))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("completion certificate changed"));
    }
}

#[test]
fn source_completion_child_future_types_fit_the_default_stack_budget() {
    fn bytes<A, F: std::future::Future>(_: impl FnOnce(A) -> F) -> usize {
        std::mem::size_of::<F>()
    }
    type Execution = (
        ReprocessExecutionContext,
        ReprocessJob,
        RecordId,
        ReprocessConfig,
        PlanningTruthMaintenanceStorage,
    );
    type Completion = (
        ReprocessExecutionContext,
        RecordId,
        RecordId,
        ReprocessConfig,
        SelectionManifest,
    );
    // Only infer real child future types. No fixture, backend, or future is
    // constructed/polled, and no alternate stack configuration is used.
    let sizes = [
        (
            "source execute",
            bytes(|(ctx, job, run, config, planning): Execution| async move {
                execute(&ctx, job, &run, SPACE, &config, None, planning).await
            }),
        ),
        (
            "source admission",
            bytes(
                |(ctx, job, run, config, selection): Completion| async move {
                    admit_and_poll(
                        &ctx,
                        &job,
                        &run,
                        SPACE,
                        &config,
                        None,
                        selection,
                        Vec::new(),
                    )
                    .await
                },
            ),
        ),
        (
            "source completion",
            bytes(
                |(ctx, job, run, config, selection): Completion| async move {
                    finish_from_receipts(
                        &ctx,
                        &job,
                        &run,
                        SPACE,
                        None,
                        &config,
                        &selection,
                        &[],
                        false,
                    )
                    .await
                },
            ),
        ),
    ];
    for (phase, size) in sizes {
        assert!(size <= 128 * 1024, "{phase} future is {size} bytes");
    }
}

#[tokio::test]
async fn completed_source_reprocess_cannot_reconstruct_a_missing_maintenance_seed() {
    for embedded in [true, false] {
        let (ctx, run) = Box::pin(setup(embedded)).await;
        let original = Box::pin(all_stages_job(&ctx)).await;
        let config = ReprocessConfig::default();
        let first = Box::pin(execute_reprocess_job(
            &ctx,
            &original.base.id,
            &run,
            SPACE,
            &config,
        ))
        .await
        .unwrap();
        assert!(!first.continuation_required);
        let completed = ctx
            .job_repo
            .get(SPACE, &original.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.status, ReprocessStatus::Completed);
        let artifacts = WorkArtifactRepository::new(ctx.storage.clone());
        let id = maintenance_id(&original.base.id);
        let seed = artifacts.get(SPACE, &id).await.unwrap().unwrap();
        let mut changed = seed.clone();
        changed.data["request_fingerprint"] = serde_json::json!("changed-original-request");
        artifacts.update(SPACE, &id, changed).await.unwrap();
        let error = Box::pin(execute_reprocess_job(
            &ctx,
            &original.base.id,
            &run,
            SPACE,
            &config,
        ))
        .await
        .unwrap_err();
        assert!(error.to_string().contains("maintenance intent changed"));
        artifacts.delete(SPACE, &id).await.unwrap();
        let error = Box::pin(execute_reprocess_job(
            &ctx,
            &original.base.id,
            &run,
            SPACE,
            &config,
        ))
        .await
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("maintenance intent is unavailable"));
        assert!(artifacts.get(SPACE, &id).await.unwrap().is_none());
        assert_eq!(
            ctx.job_repo.get(SPACE, &original.base.id).await.unwrap(),
            Some(completed)
        );
    }
}

#[test]
fn completion_request_preserves_operation_membership_independently_of_traversal_order() {
    let selection = SelectionManifest {
        job_id: RecordId::new_v4(TableName::ReprocessJob),
        request_fingerprint: "original-selection".into(),
        configuration: SpaceConfigToken {
            space_id: SPACE.into(),
            config_id: "original-config".into(),
            generation: 1,
            policy_id: "original-policy".into(),
        },
        source_count: 3,
        source_artifact_digest: "original-intents".into(),
    };
    let original = vec![
        "operation-c".to_string(),
        "operation-a".into(),
        "operation-b".into(),
    ];
    let saved = original.clone();
    let request = completion_request(&selection, &original);
    let fingerprint = request_fingerprint(&request).unwrap();
    assert_eq!(
        original, saved,
        "canonical encoding must not reorder caller authority"
    );
    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let reordered = order.map(|index| original[index].clone());
        assert_eq!(completion_request(&selection, &reordered), request);
        assert_eq!(
            request_fingerprint(&completion_request(&selection, &reordered)).unwrap(),
            fingerprint
        );
    }
    for changed in [
        vec![
            "operation-a".into(),
            "operation-b".into(),
            "operation-replacement".into(),
        ],
        vec![
            "operation-a".into(),
            "operation-b".into(),
            "operation-c".into(),
            "operation-c".into(),
        ],
        vec![
            "operation-a".into(),
            "operation-a".into(),
            "operation-c".into(),
        ],
        vec!["operation-a".into(), "operation-b".into()],
    ] {
        assert_ne!(
            request_fingerprint(&completion_request(&selection, &changed)).unwrap(),
            fingerprint
        );
    }
    let mut changed = selection;
    changed.configuration.generation += 1;
    assert_ne!(
        request_fingerprint(&completion_request(&changed, &original)).unwrap(),
        fingerprint
    );
}
