//! Shared current-source projections for retained HTTP read routes.
use super::entities::ApiError;
use crate::{auth::Principal, authz::AuthzState};
use axum::{http::StatusCode, Json};
use elephant_policy::RecordAttributes;
use elephant_storage::{
    read_eligibility::{CurrentRead, ReadEligibility},
    StorageClient,
};
use elephant_types::records::{BaseFields, HasSecurity};

type ReadResult<T> = Result<T, (StatusCode, Json<ApiError>)>;

/// Evaluate a parent/current-source envelope with the existing requested scope.
#[must_use]
pub fn authorized(
    authz: &AuthzState,
    principal: &Principal,
    base: &BaseFields,
    scope: &str,
) -> bool {
    authz
        .evaluate(
            principal,
            &RecordAttributes {
                space_id: base.space_id.clone(),
                security: base.security.clone(),
                subjects: base.subjects.clone(),
                subjects_state: base.subjects_state,
            },
            scope,
        )
        .is_ok()
}

/// Filter current records. Production route states always carry storage;
/// repository-only unit fixtures can supply only intrinsic source-less rows.
///
/// # Errors
///
/// Returns a service-unavailable error if current source support cannot be
/// loaded or rechecked, or if the required storage is unavailable.
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain guard, repository and runtime owners through the original response or state transition scope"
)]
pub async fn filter<T: CurrentRead + HasSecurity>(
    storage: Option<&StorageClient>,
    authz: &AuthzState,
    principal: &Principal,
    records: Vec<T>,
    scope: &str,
) -> ReadResult<Vec<T>> {
    if let Some(storage) = storage {
        let reader = ReadEligibility::new(storage.clone());
        let unavailable = |_| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiError::internal("Current source support unavailable")),
            )
        };
        let mut visible = Vec::with_capacity(records.len());
        for record in records {
            let root = record.read_base().clone();
            let root_allowed = authz
                .authorize_record_read(principal, &record, scope)
                .is_ok();
            // Root subjects may be derived from entity identity, relationship
            // endpoints or assertion links. Parent/source envelopes retain
            // their own exact BaseFields authorization.
            let authorize = |base: &BaseFields| {
                if base.id == root.id && base.space_id == root.space_id {
                    // Published Source reads project an immutable envelope;
                    // acceptance independently updates the live row timestamp.
                    // Source has no derived access attributes beyond its base,
                    // so authorize that envelope separately as well as the root.
                    root_allowed
                        && (*base == root
                            || (base.id.table() == elephant_types::TableName::Source
                                && authorized(authz, principal, base, scope)))
                } else {
                    authorized(authz, principal, base, scope)
                }
            };
            if let Some(mut record) = reader
                .project(record, &authorize)
                .await
                .map_err(unavailable)?
            {
                record
                    .project_action_state(&reader, &|base| {
                        authorized(
                            authz,
                            principal,
                            base,
                            elephant_storage::action_disposition::dependency_scope(base.id.table()),
                        )
                    })
                    .await
                    .map_err(unavailable)?;
                visible.push(record);
            }
        }
        reader.verify_current().await.map_err(unavailable)?;
        return Ok(visible);
    }
    #[cfg(test)]
    {
        if records.iter().all(|record| {
            record.read_base().provenance.sources.is_empty()
                && record.read_base().provenance.extractor.is_none()
        }) {
            return Ok(records
                .into_iter()
                .filter(|record| {
                    authz
                        .authorize_record_read(principal, record, scope)
                        .is_ok()
                })
                .collect());
        }
    }
    Err((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ApiError::internal("Current source support unavailable")),
    ))
}

/// Require an eligible record on a direct/raw read.
///
/// # Errors
///
/// Propagates current-record filtering errors and returns not-found when the
/// record is not eligible or authorized for the requested scope.
pub async fn require<T: CurrentRead + HasSecurity>(
    storage: Option<&StorageClient>,
    authz: &AuthzState,
    principal: &Principal,
    record: T,
    scope: &str,
) -> ReadResult<T> {
    filter(storage, authz, principal, vec![record], scope)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("Current record", "unavailable")),
            )
        })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]
    use super::*;
    use elephant_storage::{EntityRepository, RelRepository, Repository, SourceRepository};
    use elephant_types::records::{
        base::SubjectsState, entity::EntityType, rel::ClaimStatus, Entity, Rel, Source, SourceType,
    };
    use elephant_types::{Link, RecordId, TableName};

    #[tokio::test]
    async fn root_derived_subjects_do_not_replace_parent_authorization() {
        let storage = elephant_storage::StorageClient::new_embedded()
            .await
            .unwrap();
        let allowed = EntityRepository::new(storage.clone())
            .create(Entity::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Entity),
                    "subjects".into(),
                    "fixture".into(),
                ),
                EntityType::new("person"),
                "Allowed root".into(),
            ))
            .await
            .unwrap();
        let denied = EntityRepository::new(storage.clone())
            .create(Entity::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Entity),
                    "subjects".into(),
                    "fixture".into(),
                ),
                EntityType::new("person"),
                "PRIVATE ROOT CANARY".into(),
            ))
            .await
            .unwrap();
        assert!(allowed.base.subjects.is_empty());
        let authz = std::sync::Arc::new(crate::authz::AuthzService::new());
        let principal = Principal {
            subject: "reader".into(),
            space_id: Some("subjects".into()),
            scopes: vec!["read:entity".into(), "read:relationship".into()],
            subject_allowlist: Some(vec![allowed.base.id.clone()]),
            ..Principal::default()
        };
        let visible = filter(
            Some(&storage),
            &authz,
            &principal,
            vec![denied.clone(), allowed.clone()],
            "read:entity",
        )
        .await
        .unwrap();
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].base.id, allowed.base.id);
        assert!(!serde_json::to_string(&visible)
            .unwrap()
            .contains("PRIVATE ROOT CANARY"));

        let relationship = Rel {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Rel),
                "subjects".into(),
                "fixture".into(),
            ),
            subject: Link::new(allowed.base.id.clone()),
            object: Link::new(allowed.base.id.clone()),
            rel_type: "related_to".into(),
            status: ClaimStatus::Approved,
            confidence: 1.0,
            validity_window: None,
            evidence: vec![],
            temporal_audit: None,
        };
        // No BaseFields subjects: real endpoint-derived subjects authorize it.
        let relationship = RelRepository::new(storage.clone())
            .create(relationship.clone())
            .await
            .unwrap();
        let visible = filter(
            Some(&storage),
            &authz,
            &principal,
            vec![relationship.clone()],
            "read:relationship",
        )
        .await
        .unwrap();
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].base.id, relationship.base.id);

        let mut outside = relationship.clone();
        outside.base.id = RecordId::new_v4(TableName::Rel);
        outside.base.subjects = vec![Link::new(allowed.base.id.clone())];
        outside.base.subjects_state = SubjectsState::Complete;
        outside.object = Link::new(denied.base.id.clone());
        outside.rel_type = "PRIVATE ENDPOINT CANARY".into();
        let outside = RelRepository::new(storage.clone())
            .create(outside)
            .await
            .unwrap();
        let visible = filter(
            Some(&storage),
            &authz,
            &principal,
            vec![outside],
            "read:relationship",
        )
        .await
        .unwrap();
        assert!(
            visible.is_empty(),
            "partial explicit subjects cannot hide an outside endpoint"
        );

        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "subjects".into(),
                "fixture".into(),
            ),
            SourceType::Manual,
            "private-source-hash".into(),
        );
        source.base.security.level = 8;
        source.base.subjects = vec![Link::new(allowed.base.id.clone())];
        source.base.subjects_state = SubjectsState::Complete;
        let source = Repository::create(&SourceRepository::new(storage.clone()), source)
            .await
            .unwrap();
        let mut source_backed = relationship;
        source_backed.base.id = RecordId::new_v4(TableName::Rel);
        source_backed.base.provenance.sources.push(source.base.id);
        let source_backed = RelRepository::new(storage.clone())
            .create(source_backed)
            .await
            .unwrap();
        let visible = filter(
            Some(&storage),
            &authz,
            &principal,
            vec![source_backed],
            "read:relationship",
        )
        .await
        .unwrap();
        assert!(
            visible.is_empty(),
            "root authorization cannot authorize the classified parent"
        );
    }
}
