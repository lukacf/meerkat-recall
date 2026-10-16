#![allow(clippy::expect_used)]
use super::*;
use crate::{Repository, StorageClient, WorkItemRepository};
use elephant_types::records::reprocess::{
    ReprocessProgress, ReprocessTargetStage, SelectionCriteria,
};
use elephant_types::records::{BaseFields, SubjectsState};
use elephant_types::{Link, RecordId};

fn job() -> ReprocessJob {
    ReprocessJob {
        base: BaseFields::new(
            RecordId::new_v4(TableName::ReprocessJob),
            "classified-job".into(),
            "caller".into(),
        ),
        name: "classified source maintenance".into(),
        selection_criteria: SelectionCriteria::default(),
        target_stages: vec![ReprocessTargetStage::ReMaintain],
        status: ReprocessStatus::Pending,
        progress: ReprocessProgress::default(),
        priority: 3,
        budget_limit_euros: Some(2.0),
        max_sources_per_day: None,
        started_at: None,
        completed_at: None,
        error: None,
        reason: None,
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage, jobs, configurations, items in the original fixture scope through assertions and cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep this ordered conformance scenario and its failure assertions in one function."
)]
async fn contract(embedded: bool) {
    let storage = if embedded {
        Box::pin(StorageClient::new_embedded())
            .await
            .expect("embedded")
    } else {
        Box::pin(StorageClient::new_memory()).await.expect("native")
    };
    if !embedded {
        for schema in [
            concat!(
                include_str!("../../migrations/sql/031_source_publication.surql"),
                "\n",
                include_str!("../../migrations/sql/046_source_support_index_fence.surql")
            ),
            include_str!("../../migrations/sql/032_work_recovery.surql"),
            include_str!("../../migrations/sql/039_private_reference_fence.surql"),
            include_str!("../../migrations/sql/041_work_producer_origin.surql"),
        ] {
            storage
                .query_checked(schema)
                .await
                .expect("explicit fixture schema");
        }
    }
    let jobs = ReprocessJobRepository::new(storage.clone());
    assert!(
        matches!(
            jobs.prepare_creation(job()).await,
            Err(StorageError::NotFound(_))
        ),
        "missing configuration cannot become default-policy authority"
    );
    let configurations = SpaceConfigRepository::new(storage.clone());
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::SpaceConfig),
        "classified-job".into(),
        "owner".into(),
    );
    base.security.policy_id = "business-policy".into();
    base.security.level = 7;
    base.security.labels = vec!["restricted".into()];
    base.subjects = vec![Link::new(RecordId::new_v4(TableName::Entity))];
    base.subjects_state = SubjectsState::Complete;
    let configuration = configurations
        .create(SpaceConfig {
            base,
            embedding_provider: "fake".into(),
            embedding_model: "unused".into(),
            embedding_dims: 8,
            processing: None,
            purpose: None,
            skill: None,
        })
        .await
        .expect("actual classified space");
    let original = job();
    let prepared = jobs
        .prepare_creation(original.clone())
        .await
        .expect("prepare full scope");
    assert_eq!(prepared.job().base.id, original.base.id);
    assert_eq!(prepared.job().base.created_by, original.base.created_by);
    assert_eq!(prepared.job().base.security, configuration.base.security);
    assert_eq!(prepared.job().base.subjects, configuration.base.subjects);
    assert_eq!(
        prepared.job().base.subjects_state,
        configuration.base.subjects_state
    );
    assert_eq!(
        prepared.job().base.provenance,
        configuration.base.provenance
    );
    assert_eq!(prepared.job().budget_limit_euros, Some(2.0));
    let authorized_job = prepared.job().clone();
    let created = jobs
        .create_prepared_with_work(prepared)
        .await
        .expect("atomic authorized job and queue");
    assert_eq!(created, authorized_job);
    let work_id = jobs
        .ensure_work(&created)
        .await
        .expect("exact retained queue handoff");
    let items = WorkItemRepository::new(storage.clone());
    let work = items
        .get("classified-job", &work_id)
        .await
        .expect("read work")
        .expect("actual handoff");
    crate::reprocess_job_repository::verify_reprocess_handoff(&created, &work)
        .expect("full inherited envelope");
    let origin = crate::work_producer_repository::WorkProducerRepository::new(storage.clone())
        .origin_for_work(&work)
        .await
        .expect("original private finite job proof");
    assert!(matches!(
        origin,
        crate::work_producer_repository::WorkProducerOrigin::ReprocessJob {
            original_currency_restricted: true,
            ..
        }
    ));

    if !embedded {
        let read_epoch = || async {
            let mut response = storage.query_bind_checked(
                "SELECT * FROM ONLY $id",
                crate::bindings! {"id"=>surrealdb::sql::Thing::from(("canonical_epoch", surrealdb::sql::Id::String(created.base.space_id.clone())))},
            ).await.expect("read complete existing shared writer fence");
            let epoch: surrealdb::Value = response.take(0).expect("native epoch row");
            let epoch = epoch.into_inner();
            assert!(
                matches!(&epoch, surrealdb::sql::Value::Object(fields)
                    if matches!(fields.get("epoch"), Some(surrealdb::sql::Value::Strand(value)) if !value.0.is_empty())),
                "actual epoch record must contain its semantic fence"
            );
            epoch
        };
        let before = read_epoch().await;
        for _ in 0..2 {
            jobs.ensure_work(&created)
                .await
                .expect("same exact native durable handoff");
            assert_eq!(
                read_epoch().await,
                before,
                "observing an existing handoff must leave the complete epoch row unchanged"
            );
        }
        assert_eq!(
            jobs.get(&created.base.space_id, &created.base.id)
                .await
                .expect("unchanged job"),
            Some(created.clone())
        );
        assert_eq!(
            items
                .get(&work.base.space_id, &work.base.id)
                .await
                .expect("unchanged handoff"),
            Some(work.clone())
        );
    }

    let stale = jobs
        .prepare_creation(job())
        .await
        .expect("authorized original observation");
    let stale_job = stale.job().clone();
    let stale_work = super::super::intended_work(&stale_job).expect("expected queue identity");
    let mut changed = configuration.clone();
    // SpaceConfig envelopes are immutable through ordinary replacement. Use a
    // supported real configuration change after the original preparation, so
    // this regression reaches its intended atomic snapshot guard.
    changed.purpose = Some("later configured purpose".into());
    configurations
        .replace_authorized(&configuration, changed)
        .await
        .expect("concurrent guarded configuration change");
    assert!(
        jobs.create_prepared_with_work(stale).await.is_err(),
        "original authorization cannot adopt a changed configuration"
    );
    assert!(jobs
        .get("classified-job", &stale_job.base.id)
        .await
        .expect("no partial job")
        .is_none());
    assert!(items
        .get("classified-job", &stale_work.base.id)
        .await
        .expect("no partial queue")
        .is_none());
    if let Some(store) = storage.embedded_store() {
        let rows = store
            .mutate(|tx| Ok::<_, StorageError>(tx.scan_extension_rows("work_producer_origin")?))
            .await
            .expect("physical private origin inventory");
        assert!(
            !rows
                .iter()
                .any(|(_, row)| row["work_item_id"] == stale_work.base.id.to_string()),
            "failed creation cannot leave a private origin"
        );
    } else {
        let mut response = storage.query_bind_checked(
            "SELECT VALUE id FROM work_producer_origin WHERE work_item_id = $work",
            crate::bindings! { "work" => crate::surreal::thing_from_record_id(&stale_work.base.id) },
        ).await.expect("physical private origin inventory");
        let rows: Vec<surrealdb::sql::Thing> =
            response.take(0).expect("original native identities");
        assert!(
            rows.is_empty(),
            "failed creation cannot leave a private origin"
        );
    }
    assert_eq!(
        jobs.get("classified-job", &created.base.id)
            .await
            .expect("existing job unchanged"),
        Some(created)
    );
}

#[tokio::test]
async fn embedded_prepared_job_inherits_and_fences_original_named_scope() {
    Box::pin(contract(true)).await;
}

#[tokio::test]
async fn native_prepared_job_inherits_and_fences_original_named_scope() {
    Box::pin(contract(false)).await;
}
