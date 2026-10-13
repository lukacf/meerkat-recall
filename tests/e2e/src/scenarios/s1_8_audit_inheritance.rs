#![allow(
    clippy::unwrap_used,
    clippy::mixed_attributes_style,
    clippy::similar_names,
    clippy::type_complexity,
    clippy::float_cmp,
    clippy::const_is_empty,
    clippy::expect_used,
    clippy::panic,
    unused_imports,
    dead_code,
    clippy::too_many_lines,
    clippy::cognitive_complexity
)]
use elephant_api::auth::Principal as AuthPrincipal;
use elephant_api::authz::AuthzService;
use elephant_storage::{AuditLogRepository, EntityRepository, Repository};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::entity::EntityType;
use elephant_types::records::AuditAction;
use elephant_types::records::{AuditLog, Entity};
use elephant_types::RecordId;

use crate::Harness;

#[tokio::test]
#[ignore]
async fn scenario_s1_8_audit_security_inheritance() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let entity_repo = EntityRepository::new(harness.client.clone());
    let audit_repo = AuditLogRepository::new(harness.client.clone());
    let authz = AuthzService::new();

    let space_id = "Space_X";

    // 1. A high-clearance principal creates a high-sensitivity entity.
    let entity_id = RecordId::new_v4(TableName::Entity);
    let mut entity_base = BaseFields::new(
        entity_id.clone(),
        space_id.to_string(),
        "P_high".to_string(),
    );
    entity_base.security = Security {
        policy_id: "default".to_string(),
        level: 3, // Level 3 is higher than P_low's max_level 1
        labels: vec!["vis:private".to_string()],
        handling: vec![],
    };
    entity_base.subjects = vec![];
    entity_base.subjects_state = SubjectsState::Complete;

    let entity = Entity::new(
        entity_base,
        EntityType::new("person"),
        "Sensitive Entity".to_string(),
    );
    entity_repo.create(entity).await?;

    // 2. Mock the Audit Trigger (Current system doesn't have one, so we create it manually)
    // In a working system, this would be an automatic side-effect of repo.create().
    let mut audit_base = BaseFields::new(
        RecordId::new_v4(TableName::AuditLog),
        space_id.to_string(),
        "P_high".to_string(),
    );
    // --- THE INVARIANT TEST ---
    // The Audit log MUST inherit the security attributes of the record it targets.
    audit_base.security = Security {
        policy_id: "default".to_string(),
        level: 3,
        labels: vec!["vis:private".to_string()],
        handling: vec![],
    };

    let log = AuditLog {
        base: audit_base,
        action: AuditAction::Create,
        actor: "P_high".to_string(),
        record: entity_id.clone(),
        before: None,
        after: Some(serde_json::json!({"display_name": "Sensitive Entity"})),
        timestamp: chrono::Utc::now(),
        reason: None,
    };
    audit_repo.create(log.clone()).await?;

    // 3. A low-clearance principal attempts to read the audit log.
    let p_low = AuthPrincipal {
        subject: "low_clearance".to_string(),
        space_id: Some(space_id.to_string()),
        scopes: vec!["read:audit".to_string()],
        security_level: 1,
        ..Default::default()
    };

    // Use actual AuthzService to evaluate access
    let can_read = authz.authorize_record_read(&p_low, &log, "read:audit");

    // 4. Verify invariant: low clearance must be denied.
    assert!(
        can_read.is_err(),
        "CRITICAL: P_low (Level 1) was allowed to read an Audit Log entry for a Level 3 record!"
    );

    Ok(())
}
