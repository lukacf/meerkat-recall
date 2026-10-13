//! The exact shared corpus is evaluated by Elephant's independent implementation.
//! No Meerkat Rust code is linked or invoked. This is compatibility evidence,
//! not authentication, resource-policy equivalence or governed dispatch proof.

use elephant_auth::contract::{
    check_compatibility, ContractAdvertisement, ContractProfile, ContractRefusal,
    ContractRequirements, ContractVersion, Obligation, Participant, Semantic,
};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Corpus {
    contract_version: ContractVersion,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    required: serde_json::Value,
    local: serde_json::Value,
    remote: serde_json::Value,
    expected: String,
}

fn classify(refusal: ContractRefusal) -> String {
    let (kind, participant) = match refusal {
        ContractRefusal::UnknownVersion => return "unknown_version".into(),
        ContractRefusal::UnsupportedVersion(side) => ("unsupported_version", side),
        ContractRefusal::UnsupportedProfile(side) => ("unsupported_profile", side),
        ContractRefusal::AmbiguousSupport(side) => ("ambiguous_support", side),
        ContractRefusal::UnsupportedSemantic(side) => ("unsupported_semantic", side),
        ContractRefusal::UnsupportedObligation(side) => ("unsupported_obligation", side),
    };
    let side = match participant {
        Participant::Local => "local",
        Participant::Remote => "remote",
    };
    format!("{kind}_{side}")
}

#[test]
fn shared_v1_negotiation_corpus() -> Result<(), Box<dyn std::error::Error>> {
    let corpus: Corpus = serde_json::from_str(include_str!("../conformance/v1/negotiation.json"))?;
    assert_eq!(corpus.contract_version, ContractVersion::V1);
    assert_eq!(corpus.cases.len(), 14);
    let mut seen = std::collections::BTreeSet::new();
    for case in corpus.cases {
        assert!(seen.insert(case.id.clone()), "duplicate vector {}", case.id);
        let result = match (
            serde_json::from_value::<ContractRequirements>(case.required),
            serde_json::from_value::<ContractAdvertisement>(case.local),
            serde_json::from_value::<ContractAdvertisement>(case.remote),
        ) {
            (Ok(required), Ok(local), Ok(remote)) => {
                match check_compatibility(&required, &local, &remote) {
                    Ok(compatible) => {
                        assert_eq!(compatible.requirements().version, required.version);
                        assert_eq!(compatible.requirements().profile, required.profile);
                        if required.profile == ContractProfile::GovernedBufferedV1 {
                            // Independent expected wire vocabulary, not evaluator constants.
                            let baseline_semantics: std::collections::BTreeSet<Semantic> =
                                serde_json::from_str(
                                    r#"["qualified_identity","retained_work_association", "attenuating_delegation","conjunctive_decisions", "exact_operation_binding","participating_authority_fence", "durable_security_evidence","conservative_dependencies", "current_audience_release","rollback_resistance"]"#,
                                )?;
                            let baseline_obligations: std::collections::BTreeSet<Obligation> =
                                serde_json::from_str(
                                    r#"["durable_entry_receipt","durable_settlement_receipt", "current_resource_use","current_recipient_release", "preserve_dependencies","confine_secrets"]"#,
                                )?;
                            assert_eq!(compatible.requirements().semantics, baseline_semantics);
                            assert_eq!(compatible.requirements().obligations, baseline_obligations);
                        }
                        "compatible".into()
                    }
                    Err(refusal) => classify(refusal),
                }
            }
            _ => "decode_refusal".into(),
        };
        assert_eq!(result, case.expected, "vector {}", case.id);
    }
    Ok(())
}

#[test]
fn pinned_shared_corpus_has_exact_upstream_bytes() {
    use sha2::{Digest as _, Sha256};
    assert_eq!(
        hex::encode(Sha256::digest(include_bytes!(
            "../conformance/v1/negotiation.json"
        ))),
        "02777b7a941e70f226997922f5195eaf7beae30d01943f0c9cb577e301005849"
    );
}
