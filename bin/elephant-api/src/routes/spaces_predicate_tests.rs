//! Actual Phase 13 approval adapter controls against both storage backends.
#![allow(clippy::expect_used, clippy::unwrap_used)]
use super::*;
use chrono::Utc;
use elephant_types::records::work::{ArtifactStatus, WorkArtifact, WorkRun};

#[tokio::test]
#[expect(
    clippy::default_trait_access,
    clippy::large_futures,
    clippy::significant_drop_tightening,
    clippy::too_many_lines,
    reason = "Retain the original inferred defaults in the source and work-run fixtures; Retain the original test dispatch future layout and await boundaries; Keep the original test fixture and route-state owners alive through the complete assertion scope; Keep the complete authorization and publication control sequence in one test"
)]
async fn phase13_approval_authorizes_artifact_and_atomically_inherits_alias_job_envelope() {
    for embedded in [true, false] {
        let storage = if embedded {
            StorageClient::new_embedded().await
        } else {
            StorageClient::new_memory().await
        }
        .expect("storage");
        if !embedded {
            elephant_migrations::apply_schema(&storage)
                .await
                .expect("complete native schema before fixture writes");
        }
        let space = "phase13-predicate";
        let config: SpaceConfig = serde_json::from_value(serde_json::json!({
            "id":RecordId::new_v4(TableName::SpaceConfig),"space_id":space,
            "created_at":Utc::now(),"updated_at":Utc::now(),"created_by":"test",
            "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
            "provenance":{},"subjects":[],"subjects_state":"complete",
            "embedding_provider":"fake","embedding_model":"test","embedding_dims":8
        }))
        .expect("config");
        SpaceConfigRepository::new(storage.clone())
            .create(config)
            .await
            .expect("space");
        let run = WorkRun {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkRun),
                space.into(),
                "producer".into(),
            ),
            work_item: Link::new(RecordId::new_v4(TableName::WorkItem)),
            attempt: 1,
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
            success: true,
            error: None,
            metrics: Default::default(),
            worker_id: "test".into(),
            worker_version: "test".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        };
        elephant_storage::WorkRunRepository::new(storage.clone())
            .create(run.clone())
            .await
            .expect("run");
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::WorkArtifact),
            space.into(),
            "producer".into(),
        );
        base.security.level = 5;
        let artifact = WorkArtifact {
            base,
            work_run: Link::new(run.base.id),
            artifact_type: PREDICATE_PROPOSAL_ARTIFACT_TYPE.into(),
            data: serde_json::json!({"space_id":space,"predicate_id":"works_at","confidence":0.8,
                "proposed_aliases":[{"alias":"employed by","source":"test","confidence":0.8}]}),
            status: ArtifactStatus::Draft,
            validated_at: None,
            committed_at: None,
            validation_errors: None,
        };
        WorkArtifactRepository::new(storage.clone())
            .create(artifact.clone())
            .await
            .expect("artifact");
        let state = Arc::new(SpaceApiState::new(
            storage.clone(),
            Arc::new(crate::authz::AuthzService::new()),
        ));
        let mut principal = AuthPrincipal {
            subject: "approver".into(),
            space_id: Some(space.into()),
            scopes: vec!["read:predicate".into(), "write:predicate".into()],
            security_level: 0,
            ..AuthPrincipal::default()
        };
        let denied = approve_predicate_proposal(
            State(state.clone()),
            axum::Extension(principal.clone()),
            Path(artifact.base.id.to_string()),
        )
        .await;
        assert_eq!(
            denied.expect_err("classified artifact").0,
            StatusCode::FORBIDDEN
        );
        assert!(state
            .alias_repo
            .list(space, 100, None)
            .await
            .expect("aliases")
            .is_empty());
        assert!(state
            .predicate_repo
            .list(space, 100, None)
            .await
            .expect("registry")
            .is_empty());
        assert!(state
            .reprocess_job_repo
            .list(space, 100, None)
            .await
            .expect("jobs")
            .is_empty());
        principal.security_level = 5;
        let Json(alias) = approve_predicate_proposal(
            State(state.clone()),
            axum::Extension(principal),
            Path(artifact.base.id.to_string()),
        )
        .await
        .expect("approval");
        assert_eq!(alias.base.security.level, 5);
        assert_eq!(
            state
                .work_artifact_repo
                .get(space, &artifact.base.id)
                .await
                .expect("read")
                .expect("artifact")
                .status,
            ArtifactStatus::Committed
        );
        let jobs = state
            .reprocess_job_repo
            .list(space, 100, None)
            .await
            .expect("jobs");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].base.security.level, 5);
        assert_eq!(
            state
                .predicate_repo
                .list(space, 100, None)
                .await
                .expect("registry")[0]
                .base
                .security
                .level,
            5
        );
    }
}
