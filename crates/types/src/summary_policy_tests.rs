use crate::provider_binding::{ModelProfile, SpaceProcessingConfig};

use super::{SummaryPolicy, SummaryThinking};

fn profile() -> ModelProfile {
    ModelProfile {
        id: "summary-v1".into(),
        connection_id: "anthropic".into(),
        model: "claude-haiku-5-5".into(),
        model_version: "operator-v1".into(),
        dimensions: None,
        preprocessing: "text-v1".into(),
        summary: None,
    }
}

fn config() -> Result<SpaceProcessingConfig, serde_json::Error> {
    serde_json::from_value(serde_json::json!({
        "generation":1,"policy_id":"fixture","connections":{
            "anthropic":{"id":"anthropic","provider":"anthropic","account_id":"test",
                "endpoint":"https://api.anthropic.com","credential_ref":"test-only",
                "credential_generation":1,"revoked":false}},
        "profiles":{"summary-v1":profile()},
        "bindings":{"summary":{"id":"approved-summary","profile_id":"summary-v1","revoked":false}},
        "active_embedding_profile_id":null,"pending_reindex":null
    }))
}

#[test]
fn summary_absence_keeps_original_profile_bytes_and_immutable_replacement(
) -> Result<(), Box<dyn std::error::Error>> {
    let original = r#"{"id":"summary-v1","connection_id":"anthropic","model":"claude-haiku-5-5","model_version":"operator-v1","dimensions":null,"preprocessing":"text-v1"}"#;
    let decoded: ModelProfile = serde_json::from_str(original)?;
    assert_eq!(serde_json::to_string(&decoded)?, original);
    assert_eq!(decoded, profile());
    let old = config()?;
    old.validate()?;
    let mut changed = old.clone();
    changed.generation += 1;
    changed
        .profiles
        .get_mut("summary-v1")
        .ok_or("missing profile")?
        .summary = Some(SummaryPolicy::for_model("anthropic", "claude-haiku-5-5"));
    assert!(old.validate_replacement(&changed).is_err());
    let mut next = old.clone();
    next.generation += 1;
    let mut new_profile = decoded;
    new_profile.id = "summary-v2".into();
    new_profile.summary = Some(SummaryPolicy::for_model("anthropic", "claude-haiku-5-5"));
    next.profiles.insert(new_profile.id.clone(), new_profile);
    next.bindings
        .get_mut(&crate::provider_binding::ModelOperation::Summary)
        .ok_or("missing binding")?
        .profile_id = "summary-v2".into();
    old.validate_replacement(&next)?;
    assert_eq!(
        next.profiles.get("summary-v1"),
        old.profiles.get("summary-v1")
    );
    Ok(())
}

#[test]
fn summary_policy_refuses_invalid_limits_model_kind_and_operation(
) -> Result<(), Box<dyn std::error::Error>> {
    let policy = SummaryPolicy::for_model("anthropic", "claude-haiku-5-5");
    assert_eq!(policy.max_output_tokens, 4096);
    assert_eq!(policy.max_summary_length, 12_000);
    assert_eq!(policy.thinking, SummaryThinking::AdaptiveLow);
    policy.validate("anthropic", "claude-haiku-5-5")?;
    assert!(policy.validate("openai", "claude-haiku-5-5").is_err());
    assert!(policy.validate("anthropic", "claude-sonnet-5-5").is_err());
    assert!(policy.validate("anthropic", "unknown-model").is_err());
    let sonnet = SummaryPolicy::for_model("anthropic", "claude-sonnet-5-5");
    assert_eq!(sonnet.thinking, SummaryThinking::BetweenToolsLow);
    sonnet.validate("anthropic", "claude-sonnet-5-5")?;
    for limit in [0, 65_537] {
        let mut invalid = policy.clone();
        invalid.max_output_tokens = limit;
        assert!(invalid.validate("anthropic", "claude-haiku-5-5").is_err());
    }
    for limit in [0, 262_145] {
        let mut invalid = policy.clone();
        invalid.max_summary_length = limit;
        assert!(invalid.validate("anthropic", "claude-haiku-5-5").is_err());
    }
    let mut value = serde_json::to_value(&policy)?;
    value["reasoning_effort"] = "low".into();
    assert!(serde_json::from_value::<SummaryPolicy>(value).is_err());
    let mut wrong = config()?;
    wrong
        .profiles
        .get_mut("summary-v1")
        .ok_or("missing profile")?
        .summary = Some(policy);
    let binding = wrong
        .bindings
        .remove(&crate::provider_binding::ModelOperation::Summary)
        .ok_or("missing binding")?;
    wrong
        .bindings
        .insert(crate::provider_binding::ModelOperation::Extraction, binding);
    assert!(wrong.validate().is_err());
    wrong
        .profiles
        .get_mut("summary-v1")
        .ok_or("missing profile")?
        .dimensions = Some(3);
    assert!(wrong.validate().is_err());
    Ok(())
}

#[test]
fn absent_summary_preserves_embedding_profile_serialization() -> Result<(), serde_json::Error> {
    let original = r#"{"id":"embedding-v1","connection_id":"openai","model":"text-embedding-3-large","model_version":"operator-v1","dimensions":3072,"preprocessing":"text-full-v2"}"#;
    let profile: ModelProfile = serde_json::from_str(original)?;
    assert_eq!(serde_json::to_string(&profile)?, original);
    assert!(profile.summary.is_none());
    Ok(())
}
