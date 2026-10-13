//! Exact shared vectors exercised through Elephant-owned pure implementations.
//! Resource policy, current authority, real sinks and raw ingress are separate.

// Malformed frozen fixtures and unhandled fixture dimensions must fail the test.
#![allow(clippy::expect_used, clippy::panic)]

use elephant_auth::information::{
    Dependency, DependencyInventory, DerivationKind, EnvelopeRefusal, EvidenceReference,
    ProtectedEnvelope, SourcePolicySnapshot, UnknownContributor,
};
use elephant_auth::restrictions::{
    ActionRef, AudienceRef, DelegationDepth, DepthRefusal, ExecutionRestrictions, Lifetime, Match,
    ProcessorRef, ResourceDomain, SetRestriction,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use std::collections::BTreeSet;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn decode<T: DeserializeOwned>(value: &Value) -> Result<T, serde_json::Error> {
    serde_json::from_value(value.clone())
}

fn array(value: &Value) -> &[Value] {
    value.as_array().expect("fixture array")
}

fn key(value: &Value) -> &str {
    value.as_str().expect("fixture reference")
}

fn corpus() -> Value {
    let value: Value =
        serde_json::from_str(include_str!("../conformance/v1/information-flow.json"))
            .expect("shared corpus");
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["contract_version"], json!({"major":1,"minor":0}));
    value
}

fn set<T: Ord + DeserializeOwned>(spec: &Value, atoms: &Value) -> SetRestriction<T> {
    let values: Vec<_> = array(&spec["values"])
        .iter()
        .map(|id| atoms[key(id)].clone())
        .collect();
    let bound = match key(&spec["bound"]) {
        "exact" => json!({"bound":"exact","values":values}),
        "unrestricted" => {
            assert!(values.is_empty());
            json!({"bound":"unrestricted"})
        }
        other => panic!("unsupported fixture bound {other}"),
    };
    decode(&json!({"bound":bound,"unresolved":spec["unresolved"]})).expect("set claim")
}

const fn classify(result: &Match) -> &'static str {
    match result {
        Match::Matches => "matches",
        Match::OutsideBounds => "outside_bounds",
        Match::Unresolved(_) => "unresolved",
    }
}

fn set_case<T: Ord + Clone + DeserializeOwned>(case: &Value, atoms: &Value) {
    let inputs: Vec<SetRestriction<T>> = array(&case["inputs"])
        .iter()
        .map(|s| set(s, atoms))
        .collect();
    assert!(!inputs.is_empty());
    let actual = inputs
        .iter()
        .fold(SetRestriction::unrestricted(), |a, b| a.intersect(b));
    assert_eq!(actual, set(&case["expected"], atoms), "{}", case["id"]);
    for probe in array(&case["probes"]) {
        let value: T = decode(&atoms[key(&probe["value"])]).expect("probe atom");
        assert_eq!(
            classify(&actual.check(&value)),
            key(&probe["expected"]),
            "{}",
            case["id"]
        );
    }
}

fn lifetime(spec: &Value) -> Lifetime {
    let bound = if spec["bound"] == "window" {
        json!({"bound":"window","window":{"not_before_ms":spec["not_before_ms"],"expires_at_ms":spec["expires_at_ms"]}})
    } else {
        json!({"bound":spec["bound"]})
    };
    decode(&json!({"bound":bound,"unresolved":spec["unresolved"]})).expect("lifetime claim")
}

fn depth(spec: &Value) -> DelegationDepth {
    let bound = if spec["bound"] == "remaining" {
        json!({"bound":"remaining","edges":spec["remaining"]})
    } else {
        json!({"bound":spec["bound"]})
    };
    decode(&json!({"bound":bound,"unresolved":spec["unresolved"]})).expect("depth claim")
}

fn restrictions(spec: &Value, corpus: &Value) -> ExecutionRestrictions {
    let atoms = &corpus["atoms"];
    ExecutionRestrictions {
        actions: set(&spec["actions"], &atoms["actions"]),
        resource_domains: set(&spec["resource_domains"], &atoms["resource_domains"]),
        processors: set(&spec["processors"], &atoms["processors"]),
        audiences: set(&spec["audiences"], &atoms["audiences"]),
        lifetime: lifetime(&spec["lifetime"]),
        delegation_depth: depth(&spec["delegation_depth"]),
    }
}

const fn depth_error(error: &DepthRefusal) -> &'static str {
    match error {
        DepthRefusal::Exhausted => "exhausted",
        DepthRefusal::Unresolved => "unresolved",
    }
}

#[test]
fn shared_restriction_vectors_use_elephant_implementation() -> TestResult {
    let corpus = corpus();
    assert_eq!(array(&corpus["set_cases"]).len(), 9);
    assert_eq!(array(&corpus["lifetime_cases"]).len(), 3);
    assert_eq!(array(&corpus["depth_cases"]).len(), 5);
    assert_eq!(array(&corpus["attenuation_cases"]).len(), 2);
    let mut names = BTreeSet::new();
    for group in [
        "set_cases",
        "lifetime_cases",
        "depth_cases",
        "attenuation_cases",
    ] {
        for case in array(&corpus[group]) {
            assert!(names.insert(key(&case["id"])));
        }
    }
    for case in array(&corpus["set_cases"]) {
        let atoms = &corpus["atoms"][key(&case["dimension"])];
        match key(&case["dimension"]) {
            "actions" => set_case::<ActionRef>(case, atoms),
            "resource_domains" => set_case::<ResourceDomain>(case, atoms),
            "processors" => set_case::<ProcessorRef>(case, atoms),
            "audiences" => set_case::<AudienceRef>(case, atoms),
            other => panic!("unhandled dimension {other}"),
        }
    }
    for case in array(&corpus["lifetime_cases"]) {
        let actual = array(&case["inputs"])
            .iter()
            .map(lifetime)
            .fold(Lifetime::unrestricted(), |a, b| a.intersect(&b));
        assert_eq!(actual, lifetime(&case["expected"]), "{}", case["id"]);
        for probe in array(&case["probes"]) {
            assert_eq!(
                classify(&actual.check(probe["now_ms"].as_u64().expect("time"))),
                key(&probe["expected"]),
                "{}",
                case["id"]
            );
        }
    }
    for case in array(&corpus["depth_cases"]) {
        let actual = depth(&case["parent"]).child(&depth(&case["requested"]));
        if let Some(expected) = case.get("expected_error") {
            assert_eq!(
                depth_error(&actual.expect_err("refused child")),
                key(expected),
                "{}",
                case["id"]
            );
        } else {
            assert_eq!(actual?, depth(&case["expected"]), "{}", case["id"]);
        }
    }
    for case in array(&corpus["attenuation_cases"]) {
        let actual = restrictions(&case["parent"], &corpus)
            .child(&restrictions(&case["requested"], &corpus));
        if let Some(expected) = case.get("expected_error") {
            assert_eq!(
                depth_error(&actual.expect_err("refused execution ceilings")),
                key(expected),
                "{}",
                case["id"]
            );
        } else {
            assert_eq!(
                actual?,
                restrictions(&case["expected"], &corpus),
                "{}",
                case["id"]
            );
        }
    }
    Ok(())
}

fn dependencies(spec: &Value, corpus: &Value) -> DependencyInventory {
    DependencyInventory::new(
        array(&spec["records"]).iter().map(|d| Dependency {
            source: decode(&corpus["sources"][key(&d["source"])]).expect("source"),
            influence: decode(&d["influence"]).expect("influence"),
        }),
        array(&spec["unknown"]).iter().map(|id| {
            decode::<UnknownContributor>(&corpus["unknown_contributors"][key(id)])
                .expect("unknown contributor")
        }),
    )
}

fn policies(spec: &Value, corpus: &Value) -> BTreeSet<SourcePolicySnapshot> {
    array(spec)
        .iter()
        .map(|id| {
            let mut policy = corpus["policy_snapshots"][key(id)].clone();
            policy["source"] = corpus["sources"][key(&policy["source"])].clone();
            decode(&policy).expect("source-local policy")
        })
        .collect()
}

fn evidence(corpus: &Value) -> EvidenceReference {
    EvidenceReference {
        authority: decode(&corpus["atoms"]["resource_domains"]["records-A"]["authority"])
            .expect("fixture authority"),
        reference_id: "fixture-derivation-evidence".into(),
    }
}

fn envelope(spec: &Value, corpus: &Value) -> ProtectedEnvelope {
    ProtectedEnvelope::source_claim(
        dependencies(&spec["dependencies"], corpus),
        set(&spec["processors"], &corpus["atoms"]["processors"]),
        set(&spec["audiences"], &corpus["atoms"]["audiences"]),
        policies(&spec["policy_snapshots"], corpus),
        evidence(corpus),
    )
    .expect("valid fixture envelope")
}

#[test]
fn shared_dependency_and_derivation_vectors_preserve_every_input() -> TestResult {
    let corpus = corpus();
    assert_eq!(array(&corpus["dependency_cases"]).len(), 3);
    assert_eq!(array(&corpus["derivation_cases"]).len(), 4);
    let mut names = BTreeSet::new();
    for group in ["dependency_cases", "derivation_cases"] {
        for case in array(&corpus[group]) {
            assert!(names.insert(key(&case["id"])));
        }
    }
    for case in array(&corpus["dependency_cases"]) {
        let actual = array(&case["inputs"])
            .iter()
            .map(|s| dependencies(s, &corpus))
            .fold(DependencyInventory::new([], []), |a, b| a.union(&b));
        assert_eq!(
            actual,
            dependencies(&case["expected"], &corpus),
            "{}",
            case["id"]
        );
        assert_eq!(
            json!(actual.declares_complete_inventory()),
            case["expected_dependency_inventory_complete"]
        );
    }
    for case in array(&corpus["derivation_cases"]) {
        let mut inputs: Vec<_> = array(&case["inputs"])
            .iter()
            .map(|id| envelope(&corpus["envelopes"][key(id)], &corpus))
            .collect();
        let expected = &case["expected"];
        for kind in array(&case["derivations"]) {
            let actual = ProtectedEnvelope::derive(&inputs, decode(kind)?, evidence(&corpus))?;
            assert_eq!(
                actual.dependencies(),
                &dependencies(&expected["dependencies"], &corpus),
                "{}",
                case["id"]
            );
            assert_eq!(
                actual.processors(),
                &set(&expected["processors"], &corpus["atoms"]["processors"])
            );
            assert_eq!(
                actual.audiences(),
                &set(&expected["audiences"], &corpus["atoms"]["audiences"])
            );
            assert_eq!(
                actual.source_policies(),
                &policies(&expected["policy_snapshots"], &corpus)
            );
            assert_eq!(
                json!(actual.dependencies().declares_complete_inventory()),
                case["expected_dependency_inventory_complete"]
            );
            let wire = serde_json::to_value(&actual)?;
            assert_eq!(&wire["origin"]["kind"], kind);
            assert_eq!(decode::<ProtectedEnvelope>(&wire)?, actual);
            // Later derivations cannot shed earlier context or unknown contributors.
            inputs = vec![actual];
        }
    }
    Ok(())
}

#[test]
fn protected_wire_refuses_missing_extra_and_ambiguous_fields() -> TestResult {
    for wire in [
        r#"{"bound":{"bound":"unrestricted"}}"#,
        r#"{"bound":{"bound":"unrestricted","values":null},"unresolved":[]}"#,
        r#"{"bound":{"bound":"unrestricted"},"unresolved":[],"unresolved":[]}"#,
        r#"{"bound":{"bound":"unrestricted"},"unresolved":["new_kind"]}"#,
    ] {
        assert!(serde_json::from_str::<SetRestriction<String>>(wire).is_err());
    }
    assert!(serde_json::from_str::<UnknownContributor>(
        r#"{"kind":"unidentified_source","details":null}"#
    )
    .is_err());
    let corpus = corpus();
    let valid = envelope(&corpus["envelopes"]["public-alice"], &corpus);
    let mut wire = serde_json::to_value(&valid)?;
    wire["source_policies"] = json!([]);
    assert!(decode::<ProtectedEnvelope>(&wire).is_err());
    let mut wire = serde_json::to_value(&valid)?;
    wire["dependencies"]["records"] = json!([]);
    assert!(decode::<ProtectedEnvelope>(&wire).is_err());
    let mut wire = serde_json::to_value(&valid)?;
    wire["dependencies"]["future_authority"] = json!(true);
    assert!(decode::<ProtectedEnvelope>(&wire).is_err());
    let encoded = serde_json::to_string(&valid)?;
    let duplicates = format!(
        "{{\"dependencies\":{},{}",
        serde_json::to_string(valid.dependencies())?,
        &encoded[1..]
    );
    assert!(serde_json::from_str::<ProtectedEnvelope>(&duplicates).is_err());
    assert_eq!(
        ProtectedEnvelope::derive(&[], DerivationKind::Artifact, evidence(&corpus)),
        Err(EnvelopeRefusal::NoInputs)
    );
    assert!(!format!("{valid:?}").contains("resource-owner"));
    assert!(!format!("{valid:?}").contains("public-v1"));
    Ok(())
}

#[test]
fn finite_intersection_laws_do_not_widen_or_discard_unknowns() {
    use elephant_auth::restrictions::Unresolved;
    let mut claims = vec![SetRestriction::unrestricted()];
    for mask in 0_u8..8 {
        claims.push(SetRestriction::exact(
            (0_u8..3).filter(|n| mask & (1 << n) != 0),
        ));
    }
    for fact in [
        Unresolved::Absent,
        Unresolved::Unknown,
        Unresolved::Unavailable,
    ] {
        claims.push(SetRestriction::unresolved(fact));
    }
    for a in &claims {
        assert_eq!(&a.intersect(a), a);
        for b in &claims {
            let result = a.intersect(b);
            assert_eq!(result, b.intersect(a));
            for n in 0..4 {
                if result.check(&n) == Match::Matches {
                    assert_eq!(a.check(&n), Match::Matches);
                    assert_eq!(b.check(&n), Match::Matches);
                }
            }
            for c in &claims {
                assert_eq!(a.intersect(&b.intersect(c)), result.intersect(c));
            }
        }
    }
}

#[test]
fn restriction_debug_redacts_exact_actions_times_and_depths() -> TestResult {
    let action: ActionRef =
        decode(&json!({"feature":"private-feature-canary", "action":"private-action-canary"}))?;
    let time: Lifetime = decode(
        &json!({"bound":{"bound":"window","window":{"not_before_ms":87_654_321,"expires_at_ms":98_765_432}},"unresolved":[]}),
    )?;
    let depth: DelegationDepth =
        decode(&json!({"bound":{"bound":"remaining","edges":7_654_321},"unresolved":[]}))?;
    let corpus = corpus();
    let mut complete = restrictions(&corpus["attenuation_cases"][0]["parent"], &corpus);
    complete.actions = SetRestriction::exact([action.clone()]);
    complete.lifetime = time.clone();
    complete.delegation_depth = depth.clone();
    let serialized = serde_json::to_string(&complete)?;
    let debug = format!("{action:?} {time:?} {depth:?} {complete:?}");
    for canary in [
        "private-feature-canary",
        "private-action-canary",
        "87654321",
        "98765432",
        "7654321",
    ] {
        assert!(serialized.contains(canary));
        assert!(!debug.contains(canary));
    }
    Ok(())
}

fn assert_digest(bytes: &[u8], expected: &str) {
    use sha2::{Digest as _, Sha256};
    assert_eq!(hex::encode(Sha256::digest(bytes)), expected);
}

#[test]
fn corpus_bytes_match_independently_versioned_upstream() {
    assert_digest(
        include_bytes!("../conformance/v1/information-flow.json"),
        "8498e08af750fa545a2a801369345ec99c35eeccc89ffd1ea76105413133548c",
    );
}
