//! Checks current model defaults and preserves the historical catalog regression.
//!
//! The meerkat 0.8.11 port first made `gpt-5.6-sol` resolve to a provider.
//!
//! On meerkat 0.7.23 this model was uncatalogued, so the agentic extractor
//! failed at agent build time with "Cannot infer provider from model
//! 'gpt-5.6-sol'" - which is how 470 documents went permanently unextracted
//! before the pin was reverted. If a future dependency change regresses the
//! catalog (a downgrade, a pin typo, a stale lockfile), this test fails
//! before the failure reaches production extraction.

#[test]
fn pinned_catalog_resolves_gpt_5_6_sol() {
    assert!(
        meerkat_models::infer_provider("gpt-5.6-sol").is_some(),
        "gpt-5.6-sol is not in the pinned meerkat model catalog; \
         extraction agent builds will fail with 'Cannot infer provider'"
    );
}

#[test]
fn pinned_catalog_still_resolves_the_previous_extraction_model() {
    // The pre-port default (and the env-override fallback if the new model
    // misbehaves in production). Rolling the model back must stay a config
    // change, not a code gamble.
    assert!(meerkat_models::infer_provider("gpt-5.5").is_some());
}

#[test]
fn pinned_catalog_supports_current_openai_defaults() -> Result<(), Box<dyn std::error::Error>> {
    use meerkat_core::model_profile::capabilities::EffortLevel;
    use meerkat_core::Provider;

    let caps = meerkat_models::capabilities_for(Provider::OpenAI, "gpt-6.1-sol")
        .ok_or("the active default must resolve in the pinned catalog")?;
    assert_eq!(
        meerkat_models::infer_provider("gpt-6.1-sol"),
        Some(Provider::OpenAI)
    );
    assert!(caps.vision && caps.supports_structured_output);
    assert!(caps.openai_responses_params.is_some());
    assert!(!caps.supports_temperature && !caps.supports_top_p);
    assert_eq!(
        caps.effort_levels,
        &[
            EffortLevel::Low,
            EffortLevel::Medium,
            EffortLevel::High,
            EffortLevel::Xhigh,
            EffortLevel::Max,
        ]
    );
    Ok(())
}

#[test]
fn pinned_catalog_supports_current_anthropic_defaults_without_forced_tools(
) -> Result<(), Box<dyn std::error::Error>> {
    use meerkat_core::Provider;

    let caps = meerkat_models::capabilities_for(Provider::Anthropic, "claude-sonnet-5-5")
        .ok_or("the auxiliary default must resolve in the pinned catalog")?;
    assert_eq!(
        meerkat_models::infer_provider("claude-sonnet-5-5"),
        Some(Provider::Anthropic)
    );
    assert!(caps.supports_structured_output);
    assert!(!caps.supports_forced_tool_choice);
    assert!(!caps.supports_temperature && !caps.supports_top_p && !caps.supports_top_k);
    Ok(())
}
