//! Canonical writes shared by the retained HTTP timeline adapters.
use super::{
    authorize_event, ApiError, ApiResult, AuthPrincipal, Event, Json, PublicMutation, Repository,
    StatusCode, TimelineState,
};
#[cfg(test)]
use super::{Arc, BatchRequest, Path, RecordId, State, StorageClient, TableName};
use elephant_storage::canonical_commit::CanonicalRecord;
use elephant_storage::public_mutation::inherit_public_envelope;
use elephant_storage::{AssertionRepository, EntityRepository, SourceRepository};

pub(super) async fn begin(
    state: &TimelineState,
    principal: &AuthPrincipal,
    space: &str,
) -> Result<Option<PublicMutation>, (StatusCode, Json<ApiError>)> {
    state
        .authz
        .authorize_space_scope(principal, space, "write:event")
        .map_err(|error| denied(error.message))?;
    match &state.storage {
        Some(storage) => PublicMutation::begin(storage.clone(), space)
            .await
            .map(Some)
            .map_err(|error| ApiError::storage(&error)),
        None => Ok(None),
    }
}

fn denied(message: impl Into<String>) -> (StatusCode, Json<ApiError>) {
    (
        StatusCode::FORBIDDEN,
        Json(ApiError {
            code: "ACCESS_DENIED".into(),
            message: message.into(),
            details: None,
        }),
    )
}

#[expect(
    clippy::result_large_err,
    reason = "Retain the shared HTTP error representation at this boundary."
)]
fn authorize(
    state: &TimelineState,
    principal: &AuthPrincipal,
    event: &Event,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    for scope in ["read:event", "write:event"] {
        authorize_event(state, principal, event, scope)
            .map_err(|error| (StatusCode::FORBIDDEN, Json(error)))?;
    }
    Ok(())
}

pub(super) async fn authorize_inputs(
    state: &TimelineState,
    principal: &AuthPrincipal,
    mutation: &mut PublicMutation,
    event: &mut Event,
    inherit: bool,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    let Some(storage) = &state.storage else {
        return Ok(());
    };
    let original = event.base.clone();
    let mut envelope = original.clone();
    let mut records = Vec::new();
    let missing = || {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(
                "Event input is not live in the selected space",
            )),
        )
    };
    let sources = event
        .source_refs
        .iter()
        .map(|link| link.id().clone())
        .chain(event.base.provenance.sources.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>();
    for id in sources {
        let source = SourceRepository::new(storage.clone())
            .get(&event.base.space_id, &id)
            .await
            .map_err(|error| ApiError::storage(&error))?
            .ok_or_else(missing)?;
        state
            .authz
            .authorize_record_read(principal, &source, "read:source")
            .map_err(|error| denied(error.message))?;
        inherit_public_envelope(&mut envelope, &source.base)
            .map_err(|error| denied(error.to_string()))?;
        if !envelope.provenance.sources.contains(&id) {
            envelope.provenance.sources.push(id);
        }
        records.push(CanonicalRecord::Source(Box::new(source)));
    }
    for link in &event.assertion_refs {
        let assertion = AssertionRepository::new(storage.clone())
            .get(&event.base.space_id, link.id())
            .await
            .map_err(|error| ApiError::storage(&error))?
            .ok_or_else(missing)?;
        state
            .authz
            .authorize_record_read(principal, &assertion, "read:assertion")
            .map_err(|error| denied(error.message))?;
        inherit_public_envelope(&mut envelope, &assertion.base)
            .map_err(|error| denied(error.to_string()))?;
        records.push(CanonicalRecord::Assertion(Box::new(assertion)));
    }
    for subject in &original.subjects {
        let entity = EntityRepository::new(storage.clone())
            .get(&event.base.space_id, subject.id())
            .await
            .map_err(|error| ApiError::storage(&error))?
            .ok_or_else(missing)?;
        state
            .authz
            .authorize_record_read(principal, &entity, "read:entity")
            .map_err(|error| denied(error.message))?;
        inherit_public_envelope(&mut envelope, &entity.base)
            .map_err(|error| denied(error.to_string()))?;
        records.push(CanonicalRecord::Entity(Box::new(entity)));
    }
    if inherit {
        event.base = envelope;
    } else if envelope.security != original.security
        || envelope.subjects != original.subjects
        || envelope.subjects_state != original.subjects_state
    {
        return Err(denied(
            "Event envelope must cover the current evidence and participant restrictions",
        ));
    }
    for record in records {
        mutation.depend_on(record);
    }
    Ok(())
}

pub(super) async fn save(
    state: &TimelineState,
    principal: &AuthPrincipal,
    mut mutation: Option<PublicMutation>,
    observed: Option<&Event>,
    mut event: Event,
    restore: bool,
) -> ApiResult<Event> {
    state
        .authz
        .authorize_space_scope(principal, &event.base.space_id, "read:event")
        .map_err(|error| denied(error.message))?;
    if let Some(observed) = observed {
        authorize(state, principal, observed)?;
        elephant_policy::authorize_ordinary_update(&observed.base, &event.base).map_err(denied)?;
    }
    if let Some(mutation) = &mut mutation {
        authorize_inputs(state, principal, mutation, &mut event, observed.is_none()).await?;
    }
    if restore && observed.is_some() {
        event.base.deleted_at = None;
    }
    event.validate().map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(error.to_string())),
        )
    })?;
    authorize(state, principal, &event)?;
    let result = match (mutation, observed) {
        (Some(mutation), Some(observed)) => {
            mutation
                .replace(observed, event, restore, &principal.subject)
                .await
        }
        (Some(mutation), None) => mutation.create(event, &principal.subject).await,
        // Constructor without storage is reserved for existing repository unit fixtures.
        (None, Some(observed)) => {
            if restore && observed.base.deleted_at.is_some() {
                state
                    .event_repo
                    .restore_guarded(&observed.base.space_id, &observed.base.id, observed)
                    .await
                    .map_err(|error| ApiError::storage(&error))?;
            }
            state
                .event_repo
                .update_preserving_security(&observed.base.space_id, &observed.base.id, event)
                .await
        }
        (None, None) => state.event_repo.create(event).await,
    };
    result.map(Json).map_err(|error| ApiError::storage(&error))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;
    use chrono::Utc;
    use elephant_storage::{AuditLogRepository, SpaceConfigRepository, SystemEventRepository};
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::{Source, SourceType, SpaceConfig};
    use elephant_types::Link;

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
    async fn fixture(embedded: bool) -> (Arc<TimelineState>, AuthPrincipal, Source, Event) {
        let storage = if embedded {
            StorageClient::new_embedded().await
        } else {
            StorageClient::new_memory().await
        }
        .expect("storage");
        if !embedded {
            storage
                .query_checked(concat!(
                    include_str!("../../../../crates/migrations/sql/031_source_publication.surql"),
                    "\n",
                    include_str!(
                        "../../../../crates/migrations/sql/046_source_support_index_fence.surql"
                    )
                ))
                .await
                .expect("canonical schema");
        }
        let space = "canonical-timeline";
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
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                space.into(),
                "test".into(),
            ),
            SourceType::Manual,
            "original-input".into(),
        );
        source.base.security.level = 4;
        SourceRepository::new(storage.clone())
            .create(source.clone())
            .await
            .expect("source");
        let event = Event::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Event),
                space.into(),
                "test".into(),
            ),
            "Planning meeting",
            Vec::new(),
            vec![Link::new(source.base.id.clone())],
        )
        .expect("event");
        let principal = AuthPrincipal {
            subject: "editor".into(),
            space_id: Some(space.into()),
            security_level: 4,
            scopes: vec![
                "read:event".into(),
                "write:event".into(),
                "read:source".into(),
            ],
            ..AuthPrincipal::default()
        };
        (
            Arc::new(TimelineState::new(
                storage,
                Arc::new(crate::authz::AuthzService::new()),
            )),
            principal,
            source,
            event,
        )
    }

    async fn audit_count(state: &TimelineState) -> usize {
        AuditLogRepository::new(state.storage.clone().expect("storage"))
            .list("canonical-timeline", 100, None)
            .await
            .expect("audits")
            .len()
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        clippy::too_many_lines,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
    async fn timeline_handlers_inherit_evidence_and_keep_restore_batch_atomic_on_both_backends() {
        for embedded in [true, false] {
            let (state, principal, source, event) = fixture(embedded).await;
            let mut denied_principal = principal.clone();
            denied_principal.security_level = 0;
            let denied = super::super::create_event(
                State(state.clone()),
                axum::Extension(denied_principal),
                Json(event.clone()),
            )
            .await
            .expect_err("restricted source");
            assert_eq!(denied.0, StatusCode::FORBIDDEN);
            assert_eq!(audit_count(&state).await, 0);
            assert!(state
                .event_repo
                .get("canonical-timeline", &event.base.id)
                .await
                .expect("read")
                .is_none());
            let (_, Json(mut current)) = super::super::create_event(
                State(state.clone()),
                axum::Extension(principal.clone()),
                Json(event),
            )
            .await
            .expect("create");
            assert_eq!(current.base.security, source.base.security);
            assert!(current.base.provenance.sources.contains(&source.base.id));
            assert_eq!(audit_count(&state).await, 1);
            current.summary = Some("Operator annotation".into());
            let Json(current) = super::super::update_event(
                State(state.clone()),
                axum::Extension(principal.clone()),
                Path(current.base.id.to_string()),
                Json(current),
            )
            .await
            .expect("same-evidence edit");
            assert_eq!(audit_count(&state).await, 2);
            super::super::delete_event(
                State(state.clone()),
                axum::Extension(principal.clone()),
                Path(current.base.id.to_string()),
            )
            .await
            .expect("delete");
            let deleted = state
                .event_repo
                .get_with_deleted("canonical-timeline", &current.base.id)
                .await
                .expect("read")
                .expect("deleted");
            assert!(deleted.base.deleted_at.is_some());
            let mut relabeled = deleted.clone();
            relabeled.base.security.level = 0;
            let Json(batch) = super::super::batch_events(
                State(state.clone()),
                axum::Extension(principal.clone()),
                Json(BatchRequest {
                    items: vec![relabeled],
                    atomic: false,
                }),
            )
            .await
            .expect("batch result");
            assert!(batch.succeeded.is_empty());
            assert_eq!(batch.failed.len(), 1);
            assert_eq!(audit_count(&state).await, 3);
            assert!(state
                .event_repo
                .get_with_deleted("canonical-timeline", &current.base.id)
                .await
                .expect("read")
                .expect("still deleted")
                .base
                .deleted_at
                .is_some());
            let mut restore = deleted;
            restore.summary = Some("Restored with annotation".into());
            let Json(batch) = super::super::batch_events(
                State(state.clone()),
                axum::Extension(principal.clone()),
                Json(BatchRequest {
                    items: vec![restore],
                    atomic: false,
                }),
            )
            .await
            .expect("restore batch");
            assert!(batch.failed.is_empty());
            assert_eq!(batch.succeeded.len(), 1);
            assert!(batch.succeeded[0].base.deleted_at.is_none());
            assert_eq!(
                batch.succeeded[0].summary.as_deref(),
                Some("Restored with annotation")
            );
            assert_eq!(audit_count(&state).await, 4);
            super::super::delete_event(
                State(state.clone()),
                axum::Extension(principal.clone()),
                Path(current.base.id.to_string()),
            )
            .await
            .expect("second delete");
            let Json(restored) = super::super::restore_event(
                State(state.clone()),
                axum::Extension(principal),
                Path(current.base.id.to_string()),
            )
            .await
            .expect("restore endpoint");
            assert!(restored.base.deleted_at.is_none());
            assert_eq!(restored.base.security.level, 4);
            assert_eq!(audit_count(&state).await, 6);
            assert_eq!(
                SystemEventRepository::new(state.storage.clone().expect("storage"))
                    .get_after_seq("canonical-timeline", 0, 100)
                    .await
                    .expect("outbox")
                    .len(),
                6
            );
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
    async fn captured_timeline_evidence_cannot_change_before_canonical_write_on_both_backends() {
        for embedded in [true, false] {
            let (state, principal, mut source, mut event) = fixture(embedded).await;
            let mut mutation = begin(&state, &principal, "canonical-timeline")
                .await
                .expect("begin")
                .expect("production mutation");
            authorize_inputs(&state, &principal, &mut mutation, &mut event, true)
                .await
                .expect("authorized inputs");
            source.hash = "changed-input".into();
            let source_id = source.base.id.clone();
            SourceRepository::new(state.storage.clone().expect("storage"))
                .update("canonical-timeline", &source_id, source)
                .await
                .expect("concurrent input change");
            let id = event.base.id.clone();
            assert!(mutation.create(event, &principal.subject).await.is_err());
            assert!(state
                .event_repo
                .get("canonical-timeline", &id)
                .await
                .expect("read")
                .is_none());
            assert_eq!(audit_count(&state).await, 0);
            assert!(
                SystemEventRepository::new(state.storage.clone().expect("storage"))
                    .get_after_seq("canonical-timeline", 0, 100)
                    .await
                    .expect("outbox")
                    .is_empty()
            );
        }
    }
}
