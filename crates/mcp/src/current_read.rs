//! Current source support and ABAC projections used by retained MCP reads.

use elephant_policy::RecordAttributes;
use elephant_storage::read_eligibility::{CurrentRead, ReadEligibility};
use elephant_types::records::{BaseFields, HasSecurity};

use crate::handlers::ToolContext;
use crate::{McpError, McpResult};

/// Evaluate the current source/lineage envelope with the existing read scope.
#[must_use]
pub fn authorized(ctx: &ToolContext, base: &BaseFields, scope: &str) -> bool {
    ctx.authz
        .evaluate(
            &ctx.principal,
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

/// Preserve eligible independent content while dropping obsolete source support.
///
/// # Errors
/// Returns storage errors instead of turning an unavailable store into no matches.
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the eligibility reader through projection and final current-state verification."
)]
pub async fn filter<T: CurrentRead + HasSecurity>(
    ctx: &ToolContext,
    records: Vec<T>,
    scope: &str,
) -> McpResult<Vec<T>> {
    let reader = ReadEligibility::new(ctx.storage.clone());
    let mut visible = Vec::with_capacity(records.len());
    for record in records {
        let root = record.read_base().clone();
        let root_allowed = ctx
            .authz
            .authorize_record_read(&ctx.principal, &record, scope)
            .is_ok();
        let authorize = |base: &BaseFields| {
            if base.id == root.id && base.space_id == root.space_id {
                // A Source read projects its immutable published envelope after
                // checking the current envelope. Acceptance updates the current
                // row's timestamp independently of that immutable snapshot.
                // Both envelopes still need their own authorization; Source's
                // HasSecurity view is exactly its BaseFields access attributes.
                root_allowed
                    && (*base == root
                        || (base.id.table() == elephant_types::TableName::Source
                            && authorized(ctx, base, scope)))
            } else {
                authorized(ctx, base, scope)
            }
        };
        if let Some(mut record) = reader.project(record, &authorize).await? {
            record
                .project_action_state(&reader, &|base| {
                    authorized(
                        ctx,
                        base,
                        elephant_storage::action_disposition::dependency_scope(base.id.table()),
                    )
                })
                .await?;
            visible.push(record);
        }
    }
    reader.verify_current().await?;
    Ok(visible)
}

/// Require one eligible current record for a direct read.
///
/// # Errors
/// Returns unavailable for absent support or denied current lineage.
pub async fn require<T: CurrentRead + HasSecurity>(
    ctx: &ToolContext,
    record: T,
    scope: &str,
) -> McpResult<T> {
    filter(ctx, vec![record], scope)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| {
            McpError::ExecutionError("Record unavailable under current source support".into())
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
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
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
        let ctx = ToolContext::with_authz(
            storage.clone(),
            std::sync::Arc::new(crate::authz::AuthzService::new()),
            elephant_policy::Principal {
                space_id: "subjects".into(),
                scopes: std::collections::HashSet::from([
                    "read:entity".into(),
                    "read:relationship".into(),
                ]),
                clearances: vec![elephant_policy::Clearance {
                    policy_id: "default".into(),
                    max_level: 0,
                    labels: std::collections::HashSet::new(),
                    handling_allow: std::collections::HashSet::new(),
                }],
                subject_allowlist: Some(std::collections::HashSet::from([allowed.base.id.clone()])),
                purpose: std::collections::HashSet::new(),
            },
        );
        let visible = filter(&ctx, vec![denied.clone(), allowed.clone()], "read:entity")
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
        let visible = filter(&ctx, vec![relationship.clone()], "read:relationship")
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
        let visible = filter(&ctx, vec![outside], "read:relationship")
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
        let visible = filter(&ctx, vec![source_backed], "read:relationship")
            .await
            .unwrap();
        assert!(
            visible.is_empty(),
            "root authorization cannot authorize the classified parent"
        );
    }
}
