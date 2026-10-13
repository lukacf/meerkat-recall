//! Real-LLM calibration runners.
//!
//! Both tests are `#[ignore]`d and guard on `ANTHROPIC_API_KEY` so the
//! `cargo integration` / `cargo test-e2e` lanes pass keyless. Run with:
//! `ANTHROPIC_API_KEY=... ./scripts/repo-cargo test -p elephant-calibration -- --ignored --nocapture`

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use chrono::Utc;
use elephant_calibration::{
    calibration_model, disambiguation_decision_to_json, load_disambiguation_cases, load_tm_cases,
    rationale_excerpt, score_disambiguation_case, score_tm_case, sha256_hex, verdict_to_json,
    write_results, CaseResult, DisambiguationCase, SuiteResults, TmCase,
    DISAMBIGUATION_PROMPT_TEMPLATE, HARD_PASS_FLOOR, TM_PROMPT_TEMPLATE,
};
use elephant_pipeline::scheduler::{
    AgenticPromotionDisambiguator, PromotionDisambiguationConfig, PromotionDisambiguationDecision,
    PromotionDisambiguator,
};
use elephant_pipeline::truth_maintenance::adjudicator::{
    AdjudicationConfig, AdjudicationVerdict, AgenticSlotAdjudicator, SlotAdjudicator,
};
use futures::stream::{self, StreamExt};

/// Rate-limit-friendly cap on in-flight LLM calls.
const MAX_CONCURRENCY: usize = 4;

/// Ceiling for one disambiguation call; the agent's internal budget is 60s,
/// this only guards against a wedged provider connection.
const DISAMBIGUATION_TIMEOUT: Duration = Duration::from_secs(120);

fn api_key_present() -> bool {
    std::env::var("ANTHROPIC_API_KEY").is_ok_and(|key| !key.trim().is_empty())
}

fn print_table_row(id: &str, hard_pass: bool, missed: &[String], rationale: &str) {
    println!(
        "{:<32} | {:<4} | {:<50} | {}",
        id,
        if hard_pass { "PASS" } else { "FAIL" },
        rationale_excerpt(&missed.join("; "), 50),
        rationale_excerpt(rationale, 100),
    );
}

fn finish_suite(results: &SuiteResults, failed: &[String]) {
    let path = write_results(results).expect("writing results.json");
    println!(
        "\n{}: hard-pass rate {:.2} ({} cases); results written to {}",
        results.suite,
        results.hard_pass_rate,
        results.cases.len(),
        path.display()
    );
    assert!(
        results.hard_pass_rate >= HARD_PASS_FLOOR,
        "{} hard-pass rate {:.2} is below the {HARD_PASS_FLOOR:.2} floor; failed cases: {failed:?} (see {})",
        results.suite,
        results.hard_pass_rate,
        path.display()
    );
}

fn pass_rate(passed: usize, total: usize) -> f64 {
    let passed = u32::try_from(passed).expect("case count fits in u32");
    let total = u32::try_from(total).expect("case count fits in u32");
    f64::from(passed) / f64::from(total)
}

async fn adjudicate_case(
    adjudicator: &AgenticSlotAdjudicator,
    case: &TmCase,
) -> Option<AdjudicationVerdict> {
    let input = case.input.to_adjudication_input(&case.id);
    match adjudicator.adjudicate(&input).await {
        Ok(verdict) => verdict,
        Err(error) => {
            eprintln!("case {}: adjudication error: {error}", case.id);
            None
        }
    }
}

#[tokio::test]
#[ignore = "real LLM calibration; requires ANTHROPIC_API_KEY"]
async fn calibrate_tm_adjudication() {
    if !api_key_present() {
        eprintln!("skipping: ANTHROPIC_API_KEY not set");
        return;
    }

    let cases = load_tm_cases().expect("tm_adjudication.json must be valid");
    let mut config = AdjudicationConfig::default();
    if let Some(model) = calibration_model() {
        config.model = model;
    }
    let model = config.model.clone();
    let adjudicator = AgenticSlotAdjudicator::new(config);

    println!("calibrating tm_adjudication with model {model}\n");
    let verdicts: Vec<(usize, Option<AdjudicationVerdict>)> =
        stream::iter(cases.iter().enumerate())
            .map(|(index, case)| {
                let adjudicator = &adjudicator;
                async move { (index, adjudicate_case(adjudicator, case).await) }
            })
            .buffered(MAX_CONCURRENCY)
            .collect()
            .await;

    let mut case_results = Vec::with_capacity(cases.len());
    let mut failed = Vec::new();
    for (index, verdict) in verdicts {
        let case = &cases[index];
        let (result, rationale) = match verdict {
            Some(verdict) => {
                let score = score_tm_case(case, &verdict);
                let rationale = verdict.rationale.clone();
                (
                    CaseResult {
                        id: case.id.clone(),
                        hard_pass: score.hard_pass,
                        matched_expectation: score.matched_expectation,
                        missed_hard: score.missed_hard,
                        soft: Some(score.soft),
                        verdict: verdict_to_json(&verdict),
                        rationale: rationale.clone(),
                    },
                    rationale,
                )
            }
            None => (
                CaseResult {
                    id: case.id.clone(),
                    hard_pass: false,
                    matched_expectation: None,
                    missed_hard: vec!["no verdict returned".to_string()],
                    soft: None,
                    verdict: serde_json::Value::Null,
                    rationale: String::new(),
                },
                String::new(),
            ),
        };
        print_table_row(&case.id, result.hard_pass, &result.missed_hard, &rationale);
        if !result.hard_pass {
            failed.push(case.id.clone());
        }
        case_results.push(result);
    }

    let passed = case_results
        .iter()
        .filter(|result| result.hard_pass)
        .count();
    let results = SuiteResults {
        suite: "tm_adjudication".to_string(),
        model,
        prompt_sha256: sha256_hex(TM_PROMPT_TEMPLATE.as_bytes()),
        prompt_hash_source: "template_file_bytes".to_string(),
        timestamp: Utc::now(),
        hard_pass_rate: pass_rate(passed, case_results.len()),
        cases: case_results,
    };
    finish_suite(&results, &failed);
}

async fn disambiguate_case(
    disambiguator: &AgenticPromotionDisambiguator,
    case: &DisambiguationCase,
) -> Option<PromotionDisambiguationDecision> {
    let request = case.input.to_request();
    match tokio::time::timeout(DISAMBIGUATION_TIMEOUT, disambiguator.disambiguate(&request)).await {
        Ok(Ok(decision)) => decision,
        Ok(Err(error)) => {
            eprintln!("case {}: disambiguation error: {error}", case.id);
            None
        }
        Err(_) => {
            eprintln!("case {}: disambiguation timed out", case.id);
            None
        }
    }
}

#[tokio::test]
#[ignore = "real LLM calibration; requires ANTHROPIC_API_KEY"]
async fn calibrate_entity_disambiguation() {
    if !api_key_present() {
        eprintln!("skipping: ANTHROPIC_API_KEY not set");
        return;
    }

    let cases = load_disambiguation_cases().expect("entity_disambiguation.json must be valid");
    let config = PromotionDisambiguationConfig::with_model_override(calibration_model());
    let model = config.model.clone();
    let disambiguator = AgenticPromotionDisambiguator::new(config);

    println!("calibrating entity_disambiguation with model {model}\n");
    let decisions: Vec<(usize, Option<PromotionDisambiguationDecision>)> =
        stream::iter(cases.iter().enumerate())
            .map(|(index, case)| {
                let disambiguator = &disambiguator;
                async move { (index, disambiguate_case(disambiguator, case).await) }
            })
            .buffered(MAX_CONCURRENCY)
            .collect()
            .await;

    let mut case_results = Vec::with_capacity(cases.len());
    let mut failed = Vec::new();
    for (index, decision) in decisions {
        let case = &cases[index];
        let score = score_disambiguation_case(case, decision.as_ref());
        let result = CaseResult {
            id: case.id.clone(),
            hard_pass: score.hard_pass,
            matched_expectation: score.matched_expectation,
            missed_hard: score.missed_hard,
            soft: None,
            verdict: disambiguation_decision_to_json(decision.as_ref()),
            rationale: String::new(),
        };
        print_table_row(&case.id, result.hard_pass, &result.missed_hard, "");
        if !result.hard_pass {
            failed.push(case.id.clone());
        }
        case_results.push(result);
    }

    let passed = case_results
        .iter()
        .filter(|result| result.hard_pass)
        .count();
    let results = SuiteResults {
        suite: "entity_disambiguation".to_string(),
        model,
        prompt_sha256: sha256_hex(DISAMBIGUATION_PROMPT_TEMPLATE.as_bytes()),
        prompt_hash_source: "template_file_bytes".to_string(),
        timestamp: Utc::now(),
        hard_pass_rate: pass_rate(passed, case_results.len()),
        cases: case_results,
    };
    finish_suite(&results, &failed);
}
