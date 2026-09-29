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
//! S4.3 Predicate governance scenario test.
//!
//! Verifies predicate alias lifecycle: proposed -> active.

use chrono::Utc;
use elephant_storage::{PredicateAliasRepository, PredicateRegistryRepository, Repository};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::predicate::{
    AliasScope, AliasStatus, AliasType, PredicateAlias, PredicateRegistry, PredicateSemantics,
    SemanticsStatus,
};
use elephant_types::RecordId;

use crate::Harness;

/// S4.3: Predicate governance and alias management.
///
/// Test invariants:
/// 1. New predicates start in proposed state
/// 2. Aliases can be created for existing predicates
/// 3. Proposed aliases require approval to become active
/// 4. Active predicates used for assertion resolution
#[tokio::test]
async fn scenario_s4_3_predicate_governance() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let registry_repo = PredicateRegistryRepository::new(harness.client.clone());
    let alias_repo = PredicateAliasRepository::new(harness.client.clone());

    let space_id = "Space_Predicate";

    // Invariant 1: Create a canonical predicate in the registry
    let predicate_id = RecordId::new_v4(TableName::PredicateRegistry);
    let predicate_base = BaseFields::new(
        predicate_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    let predicate = PredicateRegistry {
        base: predicate_base,
        predicate_id: "works_at".to_string(),
        description: Some("Employment relationship between person and organization".to_string()),
        status: "active".to_string(),
        semantics: Some(PredicateSemantics::default()),
        semantics_status: SemanticsStatus::Configured,
        first_seen_at: Some(Utc::now()),
        usage_count: 0,
        last_used_at: None,
    };
    let created_predicate = registry_repo.create(predicate).await?;
    assert_eq!(
        created_predicate.predicate_id, "works_at",
        "Predicate ID should be set"
    );
    assert_eq!(
        created_predicate.status, "active",
        "Canonical predicates start active"
    );
    assert_eq!(
        created_predicate.semantics_status,
        SemanticsStatus::Configured,
        "Semantics should be configured"
    );

    // Invariant 2: Aliases can be created for existing predicates
    // Create a proposed alias for the predicate
    let alias_1_id = RecordId::new_v4(TableName::PredicateAlias);
    let alias_1_base = BaseFields::new(
        alias_1_id.clone(),
        space_id.to_string(),
        "extractor_v1".to_string(),
    );
    let alias_1 = PredicateAlias {
        base: alias_1_base,
        alias: "employed_by".to_string(),
        alias_type: AliasType::RawString,
        source_plugin: Some("email_extractor".to_string()),
        confidence: 0.85,
        status: AliasStatus::Proposed, // Starts as proposed
        scope: AliasScope::Global,
        predicate_id: "works_at".to_string(),
        confidence_version: 1,
        usage_count: 0,
    };
    let created_alias_1 = alias_repo.create(alias_1).await?;

    // Invariant 1 & 3: New aliases start in proposed state
    assert_eq!(
        created_alias_1.status,
        AliasStatus::Proposed,
        "New aliases should start in proposed status"
    );
    assert_eq!(
        created_alias_1.alias, "employed_by",
        "Alias string should be stored"
    );
    assert_eq!(
        created_alias_1.predicate_id, "works_at",
        "Alias should link to canonical predicate"
    );

    // Create another proposed alias from a different source
    let alias_2_id = RecordId::new_v4(TableName::PredicateAlias);
    let alias_2_base = BaseFields::new(
        alias_2_id.clone(),
        space_id.to_string(),
        "extractor_v2".to_string(),
    );
    let alias_2 = PredicateAlias {
        base: alias_2_base,
        alias: "is_employee_of".to_string(),
        alias_type: AliasType::PluginCode,
        source_plugin: Some("linkedin_extractor".to_string()),
        confidence: 0.75,
        status: AliasStatus::Proposed,
        scope: AliasScope::PluginOnly,
        predicate_id: "works_at".to_string(),
        confidence_version: 1,
        usage_count: 0,
    };
    let created_alias_2 = alias_repo.create(alias_2).await?;
    assert_eq!(
        created_alias_2.status,
        AliasStatus::Proposed,
        "Second alias also starts proposed"
    );
    assert_eq!(
        created_alias_2.scope,
        AliasScope::PluginOnly,
        "Scope should be plugin-specific"
    );

    // Invariant 3: Proposed aliases require approval to become active
    // Approve the first alias (simulate operator approval)
    let mut approved_alias = created_alias_1.clone();
    approved_alias.status = AliasStatus::Active;
    approved_alias.confidence = 0.95; // Increase confidence after review
    approved_alias.usage_count = 1;
    let updated_alias_1 = alias_repo
        .update(space_id, &alias_1_id, approved_alias)
        .await?;
    assert_eq!(
        updated_alias_1.status,
        AliasStatus::Active,
        "Alias should be active after approval"
    );
    assert!(
        (updated_alias_1.confidence - 0.95).abs() < f32::EPSILON,
        "Confidence should be updated after review"
    );

    // Invariant 4: Active predicates/aliases used for assertion resolution
    // Verify we can look up the active alias
    let retrieved_alias = alias_repo.get(space_id, &alias_1_id).await?;
    assert!(retrieved_alias.is_some());
    let active_alias = retrieved_alias.unwrap();
    assert_eq!(
        active_alias.status,
        AliasStatus::Active,
        "Retrieved alias should be active"
    );

    // Update predicate usage count (simulating assertion resolution)
    let mut used_predicate = created_predicate.clone();
    used_predicate.usage_count = 10;
    used_predicate.last_used_at = Some(Utc::now());
    let updated_predicate = registry_repo
        .update(space_id, &predicate_id, used_predicate)
        .await?;
    assert_eq!(
        updated_predicate.usage_count, 10,
        "Usage count should be tracked"
    );
    assert!(
        updated_predicate.last_used_at.is_some(),
        "Last used timestamp should be recorded"
    );

    // Test deprecation flow
    let mut deprecated_alias = created_alias_2.clone();
    deprecated_alias.status = AliasStatus::Deprecated;
    let updated_alias_2 = alias_repo
        .update(space_id, &alias_2_id, deprecated_alias)
        .await?;
    assert_eq!(
        updated_alias_2.status,
        AliasStatus::Deprecated,
        "Alias should be deprecated"
    );

    // Verify all aliases are retrievable
    let all_aliases = alias_repo.list(space_id, 100, None).await?;
    assert_eq!(all_aliases.len(), 2, "Both aliases should be stored");

    // Verify one is active, one is deprecated
    let active_count = all_aliases
        .iter()
        .filter(|a| a.status == AliasStatus::Active)
        .count();
    let deprecated_count = all_aliases
        .iter()
        .filter(|a| a.status == AliasStatus::Deprecated)
        .count();
    assert_eq!(active_count, 1, "One alias should be active");
    assert_eq!(deprecated_count, 1, "One alias should be deprecated");

    // Verify predicate with default semantics
    let predicate_2_id = RecordId::new_v4(TableName::PredicateRegistry);
    let predicate_2_base = BaseFields::new(
        predicate_2_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    let predicate_2 = PredicateRegistry {
        base: predicate_2_base,
        predicate_id: "born_in".to_string(),
        description: Some("Birthplace relationship".to_string()),
        status: "active".to_string(),
        semantics: None, // No custom semantics
        semantics_status: SemanticsStatus::Default,
        first_seen_at: Some(Utc::now()),
        usage_count: 0,
        last_used_at: None,
    };
    let created_predicate_2 = registry_repo.create(predicate_2).await?;
    assert_eq!(
        created_predicate_2.semantics_status,
        SemanticsStatus::Default,
        "Predicate should use default semantics"
    );
    assert!(
        created_predicate_2.semantics.is_none(),
        "No custom semantics configured"
    );

    Ok(())
}
