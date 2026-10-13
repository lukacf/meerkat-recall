//! Pins the reason the meerkat 0.8.11 port exists: the extraction model
//! `gpt-5.6-sol` must resolve to a provider in the pinned meerkat catalog.
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
