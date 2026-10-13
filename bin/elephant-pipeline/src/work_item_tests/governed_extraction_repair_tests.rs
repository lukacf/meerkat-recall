//! Independent whole-batch repair expectations through actual production stages.
//!
//! The HTTP server below is a CONTROLLED TRANSPORT SPY. It supplies a fixed
//! counterexample and independent exact-quote verdicts. `AgenticExtractor`,
//! provider binding/credentials/HTTP, parsing, support receipts and usage remain
//! active. These controls do not establish real-provider semantic relevance.
use super::*;
use axum::extract::{rejection::JsonRejection, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use elephant_pipeline::extraction::{
    AgenticExtractor, AgenticExtractorConfig, AssertionValidator, ExtractionBudget,
    ExtractionContext, ExtractionResult,
};
use elephant_pipeline::scheduler::execution_contract::UsageBasis;
use elephant_pipeline::scheduler::extraction_repair::EXTRACTION_BUDGET_KEY;
use elephant_storage::provider_usage_repository::{ProviderCallReceipt, ProviderUsageRepository};
use elephant_storage::source_publication::{SourceMutation, SourcePublicationRepository};
use elephant_types::publication::{OperationState, SourcePrecondition};
use elephant_types::records::work::WorkArtifact;
use grounded_stage_tests::Fixture;
use meerkat::{AgentError, AgentLlmClient, AgentToolDispatcher, LlmStreamResult, Message, ToolDef};
use meerkat_core::lifecycle::run_primitive::ProviderParamsOverride;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

#[allow(dead_code)]
#[path = "../../../../tests/recall_preview_semantics/provider_spy.rs"]
#[expect(
    clippy::duplicate_mod,
    reason = "Keep each control module's spy types and state separate from sibling controls"
)]
mod provider_spy;

const TRAVEL_TEXT: &str = "For the ferry on 18 November 2026, arrive at the terminal by 07:20. The boat departs at 08:05. The reservation is for two adults and one bicycle.";
const ARRIVAL_QUOTE: &str = "For the ferry on 18 November 2026, arrive at the terminal by 07:20.";
const DEPARTURE_QUOTE: &str = "The boat departs at 08:05.";
const SUBJECT: &str = "the ferry on 18 November 2026";
const ARRIVAL_PREDICATE: &str = "terminal arrival deadline";
const DEPARTURE_PREDICATE: &str = "departure time";

// Direct stderr writes bypass libtest's per-test capture, which is lost on abort.
// Labels are fixed control phases/case names, never source data or credentials.
// A before marker does not establish that the following future was entered.
#[inline(never)]
fn repair_phase(label: &str) {
    use std::io::Write;

    let mut stderr = std::io::stderr().lock();
    writeln!(stderr, "[governed-repair-phase] {label}").expect("write control phase");
    stderr.flush().expect("flush control phase");
}

#[derive(Debug, Clone)]
struct PhysicalCall {
    method: String,
    path: String,
    credential: String,
    body: Value,
}

#[derive(Debug)]
struct Script {
    extraction_outputs: Vec<Value>,
    malformed_support: bool,
    fail_second_extraction: bool,
    extraction_calls: usize,
    support_calls: usize,
    calls: Vec<PhysicalCall>,
    failures: Vec<String>,
}

struct RepairTransport {
    endpoint: String,
    script: Arc<Mutex<Script>>,
    task: JoinHandle<()>,
}

impl Drop for RepairTransport {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl RepairTransport {
    async fn start(extraction_outputs: Vec<Value>, malformed_support: bool) -> Self {
        assert!(!extraction_outputs.is_empty());
        let script = Arc::new(Mutex::new(Script {
            extraction_outputs,
            malformed_support,
            fail_second_extraction: false,
            extraction_calls: 0,
            support_calls: 0,
            calls: Vec::new(),
            failures: Vec::new(),
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("private loopback transport spy");
        let endpoint = format!(
            "http://{}/v1",
            listener.local_addr().expect("bound spy address")
        );
        let router = Router::new()
            .fallback(spy_dispatch)
            .with_state(script.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("owned transport spy");
        });
        Self {
            endpoint,
            script,
            task,
        }
    }

    fn calls(&self) -> Vec<PhysicalCall> {
        self.script.lock().expect("spy lock").calls.clone()
    }

    fn counts(&self) -> (usize, usize) {
        let script = self.script.lock().expect("spy lock");
        assert!(
            script.failures.is_empty(),
            "unexpected HTTP dispatch: {:?}",
            script.failures
        );
        (script.extraction_calls, script.support_calls)
    }

    fn assert_counts(&self, extraction: usize, support: usize) {
        assert_eq!(self.counts(), (extraction, support));
        let calls = self.calls();
        assert_eq!(
            calls.len(),
            extraction + support,
            "all physical requests must be accounted"
        );
        assert!(calls.iter().all(|call| call.method == "POST"));
        assert!(calls
            .iter()
            .all(|call| { call.path.ends_with("/messages") || call.path.ends_with("/responses") }));
        assert!(calls
            .iter()
            .all(|call| call.credential.contains("spy-only-SEMANTICS_SPY_")));
        assert!(calls
            .iter()
            .all(|call| call.body["model"].as_str().is_some_and(|model| [
                "claude-spy-a",
                "gpt-spy-b",
                "claude-spy-a-independent-support",
                "gpt-spy-b-independent-support"
            ]
            .contains(&model))));
    }
}

#[expect(
    clippy::format_collect,
    clippy::significant_drop_tightening,
    clippy::single_match_else,
    reason = "Keep the controlled script lock scope, explicit verdict branches and exact SSE construction"
)]
async fn spy_dispatch(
    State(script): State<Arc<Mutex<Script>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Result<Json<Value>, JsonRejection>,
) -> Response {
    let body = body.map(|Json(value)| value).unwrap_or(Value::Null);
    let credential = headers
        .get("authorization")
        .or_else(|| headers.get("x-api-key"))
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let mut script = script.lock().expect("spy lock");
    // Record every request before checking it: wrong paths, malformed JSON and
    // denied responses cannot masquerade as zero outbound calls.
    script.calls.push(PhysicalCall {
        method: method.to_string(),
        path: uri.path().to_owned(),
        credential: credential.clone(),
        body: body.clone(),
    });
    if method != Method::POST
        || !(uri.path().ends_with("/messages") || uri.path().ends_with("/responses"))
        || !body.is_object()
        || !credential.contains("spy-only-SEMANTICS_SPY_")
    {
        script
            .failures
            .push("unexpected method/path/body/credential".into());
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":{"message":"controlled request refused"}})),
        )
            .into_response();
    }
    let Some(model) = body.get("model").and_then(Value::as_str) else {
        script.failures.push("model missing".into());
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":{"message":"model missing"}})),
        )
            .into_response();
    };
    let payload = if model.ends_with("-independent-support") {
        script.support_calls += 1;
        match find_support_candidates(&body) {
            Some(candidates) => {
                let mut decisions: Vec<_> = candidates
                    .iter()
                    .map(|candidate| {
                        json!({
                            "index":candidate["index"],
                            "supported":independently_supported(candidate)
                        })
                    })
                    .collect();
                if script.malformed_support && script.support_calls == 2 {
                    // A duplicate index must be a verifier error, not a false
                    // verdict or automatic repair/approval.
                    if let Some(first) = decisions.first().cloned() {
                        decisions.push(first);
                    }
                }
                json!({"decisions":decisions})
            }
            None => {
                script
                    .failures
                    .push("support request omitted actual candidate array".into());
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error":{"message":"candidate input missing"}})),
                )
                    .into_response();
            }
        }
    } else {
        let index = script.extraction_calls;
        script.extraction_calls += 1;
        if script.fail_second_extraction && index == 1 {
            return (StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error":{"type":"api_error","message":"controlled incomplete physical request"}})))
                .into_response();
        }
        if !contains_text(&body, TRAVEL_TEXT) {
            script
                .failures
                .push("extractor omitted original complete source window".into());
        }
        let Some(output) = script.extraction_outputs.get(index).cloned() else {
            script
                .failures
                .push("unexpected additional extraction call".into());
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error":{"message":"spy plan exhausted"}})),
            )
                .into_response();
        };
        output
    };
    let text = payload.to_string();
    let events = if uri.path().ends_with("/messages") {
        vec![
            json!({"type":"message_start","message":{"id":"repair-spy-message","role":"assistant","model":model,"content":[],"usage":{"input_tokens":100,"output_tokens":0}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":50}}),
            json!({"type":"message_stop"}),
        ]
    } else {
        vec![
            json!({"type":"response.output_text.delta","delta":text}),
            json!({"type":"response.completed","response":{"id":"repair-spy-response","status":"completed","output":[{"id":"repair-spy-message","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":text,"annotations":[]}]}],"usage":{"input_tokens":100,"output_tokens":50}}}),
        ]
    };
    let stream: String = events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect();
    ([("content-type", "text/event-stream")], stream).into_response()
}

fn contains_text(value: &Value, needle: &str) -> bool {
    match value {
        Value::String(text) => text.contains(needle),
        Value::Array(values) => values.iter().any(|value| contains_text(value, needle)),
        Value::Object(values) => values.values().any(|value| contains_text(value, needle)),
        _ => false,
    }
}

fn find_support_candidates(value: &Value) -> Option<Vec<Value>> {
    match value {
        Value::String(text) => {
            let parsed: Value = serde_json::from_str(text).ok()?;
            let rows = parsed.as_array()?;
            (!rows.is_empty()
                && rows.iter().all(|row| {
                    row["index"].as_u64().is_some()
                        && row["claim"].is_object()
                        && row["quote"].is_string()
                }))
            .then(|| rows.clone())
        }
        Value::Array(values) => values.iter().find_map(find_support_candidates),
        Value::Object(values) => values.values().find_map(find_support_candidates),
        _ => None,
    }
}

/// Independent known answers, not a blanket true response or a lookup by index.
/// The deadline/departure distinction is fixed in the original D corpus.
fn independently_supported(candidate: &Value) -> bool {
    let claim = &candidate["claim"];
    let quote = candidate["quote"].as_str().unwrap_or_default();
    let expected = match (claim["predicate"].as_str(), quote) {
        (Some(ARRIVAL_PREDICATE), ARRIVAL_QUOTE) => "07:20",
        (Some(DEPARTURE_PREDICATE), DEPARTURE_QUOTE) => "08:05",
        _ => return false,
    };
    claim["subject"] == SUBJECT
        && claim["literal"] == json!({"type":"string","value":expected})
        && claim["confidence"] == json!(0.99_f64)
        && claim["quote_hash"] == AssertionValidator::hash_quote(quote)
        && claim["start"]
            == json!(TRAVEL_TEXT
                .find(quote)
                .expect("independent exact quote offset"))
        && claim["end"]
            == json!(
                TRAVEL_TEXT
                    .find(quote)
                    .expect("independent exact quote offset")
                    + quote.len()
            )
        && claim["revision"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
        && claim["node"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
        && [
            "object",
            "subject_type",
            "object_type",
            "subject_entity",
            "object_entity",
            "event_time",
            "event_precision",
            "event_title",
            "event_lineage",
            "significance",
        ]
        .iter()
        .all(|key| claim[*key].is_null())
}

fn proposal_assertion(predicate: &str, value: &str, quote: &str) -> Value {
    let start = TRAVEL_TEXT.find(quote).expect("fixed source quote");
    json!({
        "subject_text":SUBJECT,"predicate_text":predicate,"object_text":null,
        "object_literal":{"type":"string","value":value},
        "evidence":{"start":start,"end":start+quote.len(),"quote":quote},
        "confidence":0.99,"source_event_time":null,"source_event_time_precision":null,
        "event_title":null,"subject_entity_id":null,"subject_entity_type":null,
        "object_entity_id":null,"object_entity_type":null,"significance":null
    })
}

fn rejected_proposal() -> Value {
    json!({"assertions":[
        proposal_assertion(ARRIVAL_PREDICATE,"07:20",ARRIVAL_QUOTE),
        proposal_assertion(DEPARTURE_PREDICATE,"08:05",DEPARTURE_QUOTE),
        proposal_assertion(ARRIVAL_PREDICATE,"08:05",ARRIVAL_QUOTE)
    ]})
}

fn repaired_proposal() -> Value {
    json!({"assertions":[
        proposal_assertion(ARRIVAL_PREDICATE,"07:20",ARRIVAL_QUOTE),
        proposal_assertion(DEPARTURE_PREDICATE,"08:05",DEPARTURE_QUOTE)
    ]})
}

struct NeverAmbientModel;
#[async_trait::async_trait]
impl AgentLlmClient for NeverAmbientModel {
    async fn stream_response(
        &self,
        _messages: &[Message],
        _tools: &[Arc<ToolDef>],
        _max_tokens: u32,
        _temperature: Option<f32>,
        _provider_params: Option<&ProviderParamsOverride>,
    ) -> Result<LlmStreamResult, AgentError> {
        Err(AgentError::ConfigError(
            "ambient model forbidden in controlled repair test".into(),
        ))
    }
    fn provider(&self) -> meerkat::Provider {
        meerkat::Provider::Other
    }
    #[expect(
        clippy::unnecessary_literal_bound,
        reason = "Retain the original controlled model implementation lifetime signature"
    )]
    fn model(&self) -> &str {
        "never-ambient-repair-spy"
    }
}

struct NoExternalTools;
#[async_trait::async_trait]
impl AgentToolDispatcher for NoExternalTools {
    fn tools(&self) -> Arc<[Arc<ToolDef>]> {
        Vec::<Arc<ToolDef>>::new().into()
    }
    async fn dispatch(
        &self,
        _call: meerkat_core::ToolCallView<'_>,
    ) -> Result<meerkat_core::ops::ToolDispatchOutcome, meerkat::ToolError> {
        Err(meerkat::ToolError::Other(
            "unexpected external tool in repair spy".into(),
        ))
    }
}

/// This adapter substitutes only external tools. Real `run_agent` builds the
/// governed SDK clients, parses the response and independently verifies support.
struct GovernedExtractor {
    inner: AgenticExtractor,
    contexts: Arc<Mutex<Vec<(ExtractionBudget, bool)>>>,
    block_completed_repair: bool,
}
impl GovernedExtractor {
    fn new(model: &str, budget: ExtractionBudget) -> Self {
        Self {
            inner: AgenticExtractor::new(
                AgenticExtractorConfig::new("http://127.0.0.1:1/unused-tools", model)
                    .with_connect_on_init(false)
                    .with_budget(budget),
            ),
            contexts: Arc::new(Mutex::new(Vec::new())),
            block_completed_repair: false,
        }
    }
}
#[async_trait::async_trait]
impl Extractor for GovernedExtractor {
    fn manifest(&self) -> &elephant_types::records::work::PluginManifest {
        self.inner.manifest()
    }
    fn effective_budget(&self, requested: &ExtractionBudget) -> Option<ExtractionBudget> {
        self.inner.effective_budget(requested)
    }
    async fn extract(&self, ctx: ExtractionContext) -> Result<ExtractionResult, PipelineError> {
        repair_phase("extractor.enter");
        let repairing = ctx.is_repair();
        self.contexts
            .lock()
            .expect("context observer")
            .push((ctx.budget.clone(), repairing));
        repair_phase("extractor.run_agent.before");
        let result = Box::pin(self.inner.run_agent(
            ctx,
            Arc::new(NoExternalTools),
            Arc::new(NeverAmbientModel),
        ))
        .await;
        repair_phase("extractor.run_agent.after");
        if repairing && self.block_completed_repair && result.is_ok() {
            // Explicit negative boundary after the ACTUAL complete governed
            // extraction/support responses, before producer artifact creation.
            return Err(PipelineError::ConfigError(
                "controlled post-response pre-artifact configuration failure".into(),
            ));
        }
        result
    }
}

/// An exact claimed attempt. Keeping execute and settlement separate exposes
/// the requested durable interruption boundaries without inventing a new worker.
struct StageAttempt {
    item: WorkItem,
    run_id: RecordId,
    observer: Arc<elephant_pipeline::provider_usage::DurableUsageObserver>,
}

impl StageAttempt {
    async fn claim(fixture: &Fixture, id: &RecordId, lease_seconds: i64) -> Self {
        repair_phase("attempt.claim.enter");
        let items = WorkItemRepository::new(fixture.storage.clone());
        let item = items
            .get(fixture.space(), id)
            .await
            .expect("queued item")
            .expect("original item");
        assert_eq!(item.status, WorkItemStatus::Runnable);
        if let Some(due) = item.run_after {
            let delay = due.signed_duration_since(Utc::now()).num_milliseconds();
            assert!(delay <= 30_000, "test never edits stored due times");
            if delay > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(
                    u64::try_from(delay).expect("positive delay") + 5,
                ))
                .await;
            }
        }
        let item = items
            .claim_with_lease(
                fixture.space(),
                id,
                "independent-repair-oracle",
                Utc::now() + chrono::Duration::seconds(lease_seconds),
            )
            .await
            .expect("real claim")
            .expect("due original work must be claimable");
        let fence = elephant_storage::work_attempt_repository::fence_from_claim(&item)
            .expect("real attempt fence");
        let run_id = RecordId::new_v4(TableName::WorkRun);
        let mut base = item.base.clone();
        base.id = run_id.clone();
        let ledger_id = BudgetEnforcer::new(
            fixture.storage.clone(),
            elephant_types::records::ingest::Budget::default(),
            fixture.space().into(),
        )
        .reserve_cost(IngestMode::Graph, &item.kind, 7.0)
        .await
        .expect("actual durable pre-execution cost reservation");
        let mut metrics = WorkRunMetrics::default();
        metrics
            .custom
            .insert("cost_ledger_id".into(), json!(ledger_id.to_string()));
        AttemptUsage::reserved(1_000, 7.0)
            .write_metrics(&mut metrics)
            .expect("original reservation provenance");
        metrics.custom.insert("attempt_fence".into(), json!(fence));
        WorkRunRepository::new(fixture.storage.clone())
            .create(WorkRun {
                base,
                work_item: Link::new(id.clone()),
                attempt: item.attempts,
                started_at: Utc::now(),
                completed_at: None,
                success: false,
                error: None,
                metrics,
                worker_id: "independent-repair-oracle".into(),
                worker_version: "controlled-stage".into(),
                model_version: None,
                prompt_id: None,
                config_hash: None,
            })
            .await
            .expect("real original run");
        let observer = elephant_pipeline::provider_usage::DurableUsageObserver::for_attempt(
            fixture.storage.clone(),
            &run_id,
            fence,
        );
        repair_phase("attempt.claim.complete");
        Self {
            item,
            run_id,
            observer,
        }
    }

    async fn execute(
        &self,
        fixture: &Fixture,
        extractor: &dyn Extractor,
    ) -> anyhow::Result<WorkRunMetrics> {
        repair_phase("attempt.execute.before_runtime");
        let result = elephant_pipeline::provider_dispatch::with_dispatch_credentials(
            Arc::new(provider_spy::SpyCredentials),
            fixture.storage.with_activity(
                elephant_pipeline::provider_dispatch::with_dispatch_observer(
                    self.observer.clone(),
                    Box::pin(super::super::execute_work_item_with_extractor(
                        &self.item,
                        &self.run_id,
                        fixture.storage.clone(),
                        &fixture.config,
                        &TmContext::none(),
                        Some(extractor),
                    )),
                ),
            ),
        )
        .await?;
        repair_phase("attempt.execute.after_runtime");
        result
    }

    async fn usage(&self) -> AttemptUsage {
        self.observer
            .summarize(AttemptUsage::reserved(1_000, 7.0))
            .await
            .expect("actual physical receipt collection")
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the run repository scope through original usage and attempt settlement assertions"
    )]
    async fn settle(
        &self,
        fixture: &Fixture,
        result: anyhow::Result<WorkRunMetrics>,
    ) -> Result<(), FailureClass> {
        repair_phase("attempt.settle.enter");
        let usage = self.usage().await;
        repair_phase("attempt.settle.usage_observed");
        match result {
            Ok(metrics) => {
                let runs = WorkRunRepository::new(fixture.storage.clone());
                let mut run = self.run(fixture).await;
                let original = run.metrics.custom.clone();
                run.metrics = metrics;
                for (key, value) in original {
                    if let Some(replaced) = run.metrics.custom.get(&key) {
                        assert_eq!(
                            replaced, &value,
                            "success must preserve original frozen metadata"
                        );
                    } else {
                        run.metrics.custom.insert(key, value);
                    }
                }
                usage.write_metrics(&mut run.metrics).expect("actual usage");
                run.success = true;
                run.completed_at = Some(Utc::now());
                runs.update(fixture.space(), &self.run_id, run)
                    .await
                    .expect("producer success");
                elephant_pipeline::provider_usage::reconcile_finished_usage(
                    &fixture.storage,
                    fixture.space(),
                    &self.run_id,
                )
                .await
                .expect("actual settlement");
                let queue = WorkQueue::new(fixture.storage.clone(), WorkQueueConfig::default())
                    .for_attempt(&self.item)
                    .expect("original queue authority");
                complete_work_item_and_promote(
                    &queue,
                    &DagExecutor::new(fixture.storage.clone()),
                    fixture.space(),
                    &self.item.base.id,
                )
                .await
                .expect("real producer completion");
                repair_phase("attempt.settle.success_complete");
                Ok(())
            }
            Err(error) => {
                let (class, action) = source_runtime::record_failed_execution(
                    &fixture.storage,
                    &self.item,
                    &self.run_id,
                    &error,
                    &usage,
                    Utc::now(),
                    0,
                    &fixture.config,
                )
                .await
                .expect("production run usage/source failure transition");
                source_runtime::apply_failed_disposition(
                    &fixture.storage,
                    &self.item,
                    &error,
                    class,
                    action,
                )
                .await
                .expect("production queue disposition");
                repair_phase("attempt.settle.failure_complete");
                Err(class)
            }
        }
    }

    async fn run(&self, fixture: &Fixture) -> WorkRun {
        WorkRunRepository::new(fixture.storage.clone())
            .get(fixture.space(), &self.run_id)
            .await
            .expect("run read")
            .expect("retained original run")
    }

    async fn candidate(&self, fixture: &Fixture) -> WorkArtifact {
        let artifacts = WorkArtifactRepository::new(fixture.storage.clone())
            .find_by_work_run(&self.run_id)
            .await
            .expect("actual artifacts");
        let candidates: Vec<_> = artifacts
            .into_iter()
            .filter(|a| a.artifact_type == KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE)
            .collect();
        assert_eq!(candidates.len(), 1, "one complete producer proposal");
        candidates.into_iter().next().expect("one proposal")
    }
}

fn hard_budget() -> ExtractionBudget {
    ExtractionBudget {
        max_tokens: 4_000,
        max_wall_time_secs: 120,
    }
}

async fn repair_fixture(
    embedded: bool,
    outputs: Vec<Value>,
    malformed: bool,
) -> (Fixture, RepairTransport, GovernedExtractor, RecordId) {
    repair_phase("fixture.transport.before");
    let transport = RepairTransport::start(outputs, malformed).await;
    repair_phase("fixture.transport.ready");
    // Both named spaces and both real backend implementations are exercised.
    let space = if embedded {
        "semantics-business"
    } else {
        "semantics-household"
    };
    repair_phase("fixture.source.before");
    let mut fixture = Box::pin(Fixture::new_with_text_and_generation_endpoint(
        space,
        embedded,
        TRAVEL_TEXT,
        Some(&transport.endpoint),
    ))
    .await;
    repair_phase("fixture.source.ready");
    fixture.config.transient_retry_base_delay_secs = 1;
    fixture.config.transient_retry_max_delay_secs = 1;
    fixture.config.transient_retry_max_attempts = 3;
    let model = if embedded {
        "gpt-spy-b"
    } else {
        "claude-spy-a"
    };
    let extractor = GovernedExtractor::new(model, hard_budget());
    repair_phase("fixture.advance.before");
    let item = Box::pin(fixture.advance_to(EXTRACT_ASSERTIONS_KIND)).await;
    repair_phase("fixture.advance.ready");
    (fixture, transport, extractor, item.base.id)
}

async fn operation(fixture: &Fixture) -> elephant_types::publication::OperationReceipt {
    SourcePublicationRepository::new(fixture.storage.clone())
        .operation(fixture.space(), &fixture.accepted.operation.operation_id)
        .await
        .expect("source operation")
        .expect("original accepted operation")
}

async fn no_canonical(fixture: &Fixture) {
    assert!(
        AssertionRepository::new(fixture.storage.clone())
            .list(fixture.space(), 100, None)
            .await
            .expect("assertion inventory")
            .is_empty(),
        "no canonical claims before complete publication"
    );
    assert!(
        EntityRepository::new(fixture.storage.clone())
            .list(fixture.space(), 100, None)
            .await
            .expect("entity inventory")
            .is_empty(),
        "no partial entity promotion"
    );
    assert_ne!(operation(fixture).await.state, OperationState::Completed);
}

async fn no_bundle(fixture: &Fixture, run_id: &RecordId) {
    assert!(
        WorkArtifactRepository::new(fixture.storage.clone())
            .find_by_work_run(run_id)
            .await
            .expect("run artifacts")
            .iter()
            .all(|artifact| artifact.artifact_type != COMMIT_BUNDLE_ARTIFACT_TYPE),
        "a failed whole proposal must never acquire an empty or partial bundle"
    );
}

async fn assert_terminal(fixture: &Fixture, item_id: &RecordId, attempts: u32) {
    let item = WorkItemRepository::new(fixture.storage.clone())
        .get(fixture.space(), item_id)
        .await
        .expect("terminal queue lookup")
        .expect("original queue item retained");
    assert_eq!(item.status, WorkItemStatus::Failed);
    assert_eq!(
        item.attempts, attempts,
        "terminal failure cannot refund charged work"
    );
    assert_eq!(
        operation(fixture).await.state,
        OperationState::FailedTerminal,
        "source and queue must use the same selected failure disposition"
    );
    no_canonical(fixture).await;
    assert!(
        WorkItemRepository::new(fixture.storage.clone())
            .claim_with_lease(
                fixture.space(),
                item_id,
                "forbidden-third-attempt",
                Utc::now() + chrono::Duration::minutes(1),
            )
            .await
            .expect("terminal claim refusal")
            .is_none(),
        "no automatic third call"
    );
}

async fn rejected_first(
    fixture: &Fixture,
    extractor: &dyn Extractor,
    item: &RecordId,
) -> (StageAttempt, Value) {
    repair_phase("rejected_first.claim.before");
    let attempt = Box::pin(StageAttempt::claim(fixture, item, 90)).await;
    repair_phase("rejected_first.execute.before");
    let result = Box::pin(attempt.execute(fixture, extractor)).await;
    repair_phase("rejected_first.execute.after");
    assert_eq!(
        classify_failure(result.as_ref().expect_err("whole proposal must reject")),
        FailureClass::Transient
    );
    let artifact = attempt.candidate(fixture).await;
    assert_eq!(artifact.status, ArtifactStatus::Rejected);
    assert_eq!(
        artifact.data["assertions"]
            .as_array()
            .expect("complete proposal")
            .len(),
        3
    );
    let claims = artifact.data["assertions"].as_array().expect("claims");
    assert!(claims[0]["support_verification"].is_object());
    assert!(claims[1]["support_verification"].is_object());
    assert!(claims[2]["support_verification"].is_null());
    let frozen_artifact = json!(artifact);
    assert_eq!(
        Box::pin(attempt.settle(fixture, result)).await,
        Err(FailureClass::Transient)
    );
    let run = attempt.run(fixture).await;
    assert!(!run.success && run.completed_at.is_some());
    assert!(run.metrics.custom.contains_key(EXTRACTION_BUDGET_KEY));
    let usage = AttemptUsage::from_metrics(&run.metrics)
        .expect("typed usage")
        .expect("usage retained");
    assert_eq!(usage.basis, UsageBasis::Measured);
    assert_eq!(
        usage.measured_tokens,
        Some(300),
        "both physical model calls counted exactly once"
    );
    assert_eq!(
        operation(fixture).await.state,
        OperationState::FailedRetryable
    );
    no_bundle(fixture, &attempt.run_id).await;
    no_canonical(fixture).await;
    (attempt, frozen_artifact)
}

async fn history_unchanged(fixture: &Fixture, first: &StageAttempt, snapshot: &Value) {
    assert_eq!(
        &json!(first.candidate(fixture).await),
        snapshot,
        "original rejected proposal is immutable history"
    );
    assert!(
        !first.run(fixture).await.success,
        "old rejected run must never be relabelled successful"
    );
    no_bundle(fixture, &first.run_id).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn complete_known_answer(fixture: &Fixture, candidate: &WorkArtifact) {
    no_canonical(fixture).await;
    let manifests =
        elephant_storage::source_publication_manifest::SourcePublicationManifestRepository::new(
            fixture.storage.clone(),
        );
    let original_manifest = manifests
        .get(fixture.space(), &fixture.accepted.publication)
        .await
        .expect("original accepted-source manifest")
        .expect("closed original manifest");
    assert_eq!(original_manifest.documents.len(), 1);
    assert_eq!(original_manifest.bundles.len(), 1);
    let expected_revision = original_manifest.documents[0].doc_revision_id.clone();
    let expected_node = original_manifest.bundles[0].node_id.clone();
    assert_eq!(original_manifest.bundles[0].revision_id, expected_revision);
    assert_eq!(
        original_manifest.documents[0].publication,
        fixture.accepted.publication
    );
    assert_eq!(
        candidate.data["source_id"],
        fixture.accepted.source.base.id.to_string()
    );
    assert_eq!(
        candidate.data["_publication"],
        json!(fixture.accepted.publication)
    );
    assert_eq!(candidate.data["revision_id"], expected_revision.to_string());
    assert_eq!(candidate.data["node_id"], expected_node.to_string());
    Box::pin(fixture.finish_publication()).await;
    assert_eq!(
        json!(manifests
            .get(fixture.space(), &fixture.accepted.publication)
            .await
            .expect("manifest after publication")
            .expect("original manifest")),
        json!(original_manifest),
        "repair cannot shrink, replace or bypass the original closed manifest"
    );
    let assertions = AssertionRepository::new(fixture.storage.clone())
        .list(fixture.space(), 100, None)
        .await
        .expect("published assertions");
    assert_eq!(
        assertions.len(),
        2,
        "entire independently verified proposal must publish"
    );
    for (predicate, literal, quote) in [
        (ARRIVAL_PREDICATE, "07:20", ARRIVAL_QUOTE),
        (DEPARTURE_PREDICATE, "08:05", DEPARTURE_QUOTE),
    ] {
        let found: Vec<_> = assertions
            .iter()
            .filter(|claim| {
                claim.subject_text == SUBJECT
                    && claim.predicate_raw == predicate
                    && claim.object_literal == Some(json!(literal))
            })
            .collect();
        assert_eq!(found.len(), 1, "known answer must exist exactly once");
        let claim = found[0];
        assert_eq!(claim.evidence.revision.id(), &expected_revision);
        assert_eq!(
            claim.evidence.node.as_ref().expect("exact node").id(),
            &expected_node
        );
        let start = TRAVEL_TEXT.find(quote).expect("known quote");
        assert_eq!(
            usize::try_from(claim.evidence.start).expect("offset"),
            start
        );
        assert_eq!(
            usize::try_from(claim.evidence.end).expect("offset"),
            start + quote.len()
        );
        assert_eq!(
            claim.evidence.quote_hash,
            AssertionValidator::hash_quote(quote)
        );
    }
    assert!(!assertions
        .iter()
        .any(|claim| claim.predicate_raw == ARRIVAL_PREDICATE
            && claim.object_literal == Some(json!("08:05"))));
    let completed = operation(fixture).await;
    assert_eq!(completed.state, OperationState::Completed);
    assert_eq!(
        completed.published_revision.as_deref(),
        Some(fixture.accepted.publication.revision_id.as_str())
    );
    eprintln!("controlled evidence home/travel@travel-r1#h-arrival -> {expected_revision}/{expected_node}; no provider semantic acceptance");
}

async fn overwrite_partial_receipt(fixture: &Fixture, receipt: &ProviderCallReceipt) {
    // Explicit storage-fault injection: the original physical call remains the
    // same, but its final usage observation is unavailable after interruption.
    // This is not a fabricated extra HTTP call or a zero-cost estimate.
    let payload = serde_json::to_string(receipt).expect("receipt fault payload");
    let id = format!("provider_call_receipt:{}", receipt.call_id);
    if let Some(store) = fixture.storage.embedded_store() {
        store
            .mutate(|tx| {
                let (_, mut row) = tx
                    .scan_extension_rows("provider_call_receipt")?
                    .into_iter()
                    .find(|(_, row)| row["id"] == id)
                    .ok_or_else(|| elephant_storage::StorageError::NotFound(id.clone()))?;
                row["payload"] = json!(payload);
                tx.upsert_extension_row("provider_call_receipt", row)?;
                Ok(())
            })
            .await
            .expect("exact existing receipt fault");
    } else {
        fixture.storage.query_bind_checked(
            "UPDATE type::thing('provider_call_receipt', $call) SET payload=$payload WHERE space_id=$space AND scope_id=$scope;",
            std::collections::BTreeMap::from([
                ("call".to_owned(), receipt.call_id.clone()),
                ("payload".to_owned(), payload),
                ("space".to_owned(), fixture.space().to_owned()),
                ("scope".to_owned(), receipt.scope_id.clone()),
            ]),
        ).await.expect("exact existing receipt fault");
    }
}

async fn expire_and_recover(fixture: &Fixture, attempt: &StageAttempt) {
    let expiry = attempt
        .item
        .lease_expires_at
        .expect("bounded original lease");
    let delay = expiry.signed_duration_since(Utc::now()).num_milliseconds();
    assert!(
        delay <= 15_000,
        "interruption case must use its declared short real lease"
    );
    if delay > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(
            u64::try_from(delay).expect("positive expiry delay") + 5,
        ))
        .await;
    }
    WorkItemRepository::new(fixture.storage.clone())
        .release_expired_lease(fixture.space(), &attempt.item.base.id)
        .await
        .expect("actual expired claim recovery")
        .expect("expired original claim recovered");
    let run = attempt.run(fixture).await;
    assert!(run.completed_at.is_none());
    assert!(WorkRunRepository::new(fixture.storage.clone())
        .finish_abandoned(&run)
        .await
        .expect("real abandoned run recovery"));
    let failed = attempt.run(fixture).await;
    assert!(!failed.success && failed.completed_at.is_some());
}

async fn inject_foreign_history(fixture: &Fixture, first: &StageAttempt, cold: bool) {
    let other_space = if fixture.space() == "semantics-household" {
        "semantics-business"
    } else {
        "semantics-household"
    };
    ensure_fixture_space(&fixture.storage, other_space, "foreign-history-oracle").await;
    let mut foreign = first.run(fixture).await;
    foreign.base.id = RecordId::new_v4(TableName::WorkRun);
    foreign.base.space_id = other_space.into();
    // Repeated attempt number with another genuine-looking generation must not
    // be selected by max(attempt), nor assumed to consume zero because it is cold.
    let mut fence = foreign.metrics.custom["attempt_fence"].clone();
    fence["space_id"] = json!(other_space);
    fence["generation"] = json!(100);
    fence["lease_token"] = json!("foreign-original-token");
    foreign.metrics.custom.insert("attempt_fence".into(), fence);
    assert_eq!(foreign.attempt, 1);
    if cold {
        let original_id = foreign.base.id.clone();
        let mut base = foreign.base.clone();
        base.id = RecordId::new_v4(TableName::WorkRunArchive);
        elephant_storage::work_retention::import_run_archive(
            &fixture.storage,
            elephant_types::records::work::WorkRunArchive {
                base,
                original_id,
                archived_at: Utc::now(),
                record: foreign,
            },
        )
        .await
        .expect("controlled cold history import");
    } else {
        WorkRunRepository::new(fixture.storage.clone())
            .create(foreign)
            .await
            .expect("controlled foreign hot history");
    }
    assert_eq!(
        WorkRunRepository::new(fixture.storage.clone())
            .find_by_work_item(&first.item.base.id)
            .await
            .expect("hot/cold history")
            .len(),
        2
    );
}

async fn physical_repair_history_absent(
    fixture: &Fixture,
    runs: &[RecordId],
    artifacts: &[RecordId],
) {
    if let Some(store) = fixture.storage.embedded_store() {
        for id in runs {
            assert!(store
                .get::<WorkRun>(fixture.space(), id, true)
                .await
                .expect("physical run")
                .is_none());
            assert!(store
                .scan::<elephant_types::records::work::WorkRunArchive>()
                .await
                .expect("physical cold runs")
                .iter()
                .all(|r| &r.original_id != id));
        }
        for id in artifacts {
            assert!(store
                .get::<WorkArtifact>(fixture.space(), id, true)
                .await
                .expect("physical artifact")
                .is_none());
        }
    } else {
        for id in runs.iter().chain(artifacts) {
            let mut response = fixture.storage.query_bind_checked(
                "SELECT VALUE type::string(id) FROM $id; SELECT VALUE type::string(id) FROM work_run_archive WHERE original_id=$id;",
                std::collections::BTreeMap::from([(
                    "id".to_owned(),
                    elephant_storage::surreal::thing_from_record_id(id),
                )]),
            ).await.expect("physical hot/cold exact-ID inspection");
            let hot: Vec<String> = response.take(0).expect("physical hot result");
            let cold: Vec<String> = response.take(1).expect("physical cold result");
            assert!(
                hot.is_empty() && cold.is_empty(),
                "erasure must physically remove old and repaired raw history"
            );
        }
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn erasure_and_restore(fixture: &Fixture, first: &StageAttempt, second: &StageAttempt) {
    use elephant_storage::snapshot::{export_named, restore_named, SnapshotRetention};
    use elephant_storage::source_intake::{IntakeControl, PreparedSourceIntake};
    let text = "Independent source: this record must survive travel-source erasure.";
    let mut source = Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            fixture.space().into(),
            "independent-oracle".into(),
        ),
        SourceType::Manual,
        AssertionValidator::hash_quote(text),
    );
    source.external_id = Some(format!("independent:{}", uuid::Uuid::new_v4()));
    source
        .set_manual_doc_template(&ManualDocTemplate {
            doc_id: RecordId::new_v4(TableName::Doc).to_string(),
            title: "Independent source".into(),
            text: text.into(),
            sidecar_text: None,
            media_attachments: vec![],
            rendition_key: "independent".into(),
            metadata: json!({}),
        })
        .expect("independent source input");
    let independent = Box::pin(PreparedSourceIntake::prepare(
        &fixture.storage,
        source,
        IntakeControl {
            revision_id: Some("independent-r1".into()),
            idempotency_key: Some("independent-repair-erasure".into()),
            expected_generation: None,
        },
    ))
    .await
    .expect("independent prepare");
    let independent = Box::pin(independent.accept(&fixture.storage))
        .await
        .expect("independent acceptance");
    let independent_source = json!(SourceRepository::new(fixture.storage.clone())
        .get_with_deleted(fixture.space(), &independent.source.base.id)
        .await
        .expect("independent snapshot"));
    let artifacts = [
        first.candidate(fixture).await.base.id,
        second.candidate(fixture).await.base.id,
    ];
    let runs = [first.run_id.clone(), second.run_id.clone()];
    let temp = tempfile::tempdir().expect("owned finite backup directory");
    let backup = temp.path().join("before-erasure");
    let journal_id = fixture
        .storage
        .erasure_journal()
        .expect("original journal")
        .identity()
        .to_owned();
    let manifest = export_named(
        &fixture.storage,
        &backup,
        "repair-history",
        &SnapshotRetention {
            retention_seconds: 600,
            quiesce_timeout_seconds: 30,
        },
    )
    .await
    .expect("real finite-retention named backup");
    assert_eq!(manifest.journal_id, journal_id);
    assert_eq!(
        manifest
            .expires_at
            .signed_duration_since(manifest.created_at)
            .num_seconds(),
        600,
        "physical erasure retains the declared finite backup deadline"
    );
    let repository = SourcePublicationRepository::new(fixture.storage.clone());
    let head = repository
        .head(fixture.space(), &fixture.accepted.source.base.id)
        .await
        .expect("current head")
        .expect("accepted head");
    let request = SourceMutation {
        space_id: fixture.space().into(),
        source_id: fixture.accepted.source.base.id.clone(),
        expected: SourcePrecondition::Generation(head.generation),
        idempotency_key: "erase-rejected-and-repaired-history".into(),
        request_fingerprint: "erase-rejected-and-repaired-history".into(),
        actor: "independent-oracle".into(),
        authorized_document: None,
        authorized: repository
            .authorization_snapshot(fixture.space(), &fixture.accepted.source.base.id)
            .await
            .expect("current source authorization")
            .expect("source snapshot"),
    };
    let erased = Box::pin(repository.erase_live(request))
        .await
        .expect("actual journal and physical erasure");
    assert_eq!(erased.state, OperationState::Erased);
    assert!(SourceRepository::new(fixture.storage.clone())
        .get_with_deleted(fixture.space(), &fixture.accepted.source.base.id)
        .await
        .expect("physical source inspection")
        .is_none());
    physical_repair_history_absent(fixture, &runs, &artifacts).await;
    assert!(fixture
        .storage
        .erasure_journal()
        .expect("original journal")
        .intents()
        .expect("journal intents")
        .iter()
        .any(|intent| intent.source_id == fixture.accepted.source.base.id
            && intent.space_id == fixture.space()));
    // Restore is additional barrier coverage, not substituted for physical erasure.
    restore_named(
        &fixture.storage,
        &backup,
        fixture.storage.erasure_journal().expect("original journal"),
    )
    .await
    .expect("original journal barrier replays erasure before Ready");
    assert!(fixture.storage.recovery_status().ready);
    assert_eq!(
        fixture
            .storage
            .erasure_journal()
            .expect("original journal")
            .identity(),
        journal_id
    );
    physical_repair_history_absent(fixture, &runs, &artifacts).await;
    assert!(SourceRepository::new(fixture.storage.clone())
        .get_with_deleted(fixture.space(), &fixture.accepted.source.base.id)
        .await
        .expect("source remains physically absent after restore")
        .is_none());
    assert_eq!(
        json!(SourceRepository::new(fixture.storage.clone())
            .get_with_deleted(fixture.space(), &independent.source.base.id)
            .await
            .expect("independent source after erase and restore")),
        independent_source
    );
}

/// Twenty case IDs were fixed before implementation/results in independent
/// expectations v2 (996f62013cdaa76f9fc12d029f6fff5ded89969c4ad99107925f2fa5b2a156ed).
/// Every case runs on native `SurrealDB` memory and elephant-db. This selector is
/// one finite stage-control batch; its pass never counts as provider acceptance.
#[tokio::test]
#[expect(
    clippy::items_after_statements,
    reason = "Keep local diagnostic types and case definitions beside the control phase that uses them"
)]
async fn governed_whole_batch_repair_preserves_history_authority_and_completion() {
    repair_phase("batch.enter");
    // Type-only checks do not construct or poll a possibly oversized future.
    // The original selector still exercises every phase on the default stack.
    fn phase_bytes<F: std::future::Future>(_: impl FnOnce(RepairCase) -> F) -> usize {
        std::mem::size_of::<F>()
    }
    fn future_bytes<F: std::future::Future>(_: impl FnOnce() -> F) -> usize {
        std::mem::size_of::<F>()
    }
    for (name, bytes) in [
        ("first_pass_empty", phase_bytes(first_pass_empty_case)),
        (
            "same_live_run_replay",
            phase_bytes(same_live_run_replay_case),
        ),
        (
            "retry_budget_exhausted",
            phase_bytes(retry_budget_exhausted_case),
        ),
        (
            "token_or_time_exhausted",
            phase_bytes(token_or_time_exhausted_case),
        ),
        (
            "crash_after_rejection",
            phase_bytes(crash_after_rejection_case),
        ),
        ("retry_repair", phase_bytes(retry_repair_case)),
        (
            "fixture",
            future_bytes(|| repair_fixture(false, Vec::new(), false)),
        ),
        (
            "router",
            future_bytes(|| repair_case_router(false, "repair_success")),
        ),
        (
            "unknown_usage",
            future_bytes(|| unknown_usage_retains_real_reservation(false)),
        ),
    ] {
        assert!(
            bytes <= 64 * 1024,
            "governed repair {name} future is {bytes} bytes; box its growing phase boundary"
        );
    }
    repair_phase("batch.size_guards.complete");
    const CASES: [&str; 20] = [
        "repair_success",
        "repeat_rejection",
        "empty_repair",
        "first_pass_empty",
        "retry_budget_exhausted",
        "token_or_time_exhausted",
        "unknown_usage",
        "revoked_before_retry",
        "wrong_space_history",
        "changed_input_or_authority",
        "credential_rotation",
        "crash_after_rejection",
        "same_live_run_replay",
        "malformed_support_reply",
        "mutated_history_or_receipt",
        "erasure_with_repair_history",
        "completed_producer_validator_crash",
        "expired_second_repair_producer",
        "archived_duplicate_attempt_number",
        "remaining_budget_decreases",
    ];
    for embedded in [false, true] {
        repair_phase(if embedded {
            "backend.elephant-db"
        } else {
            "backend.native-surreal-memory"
        });
        for case in CASES {
            eprintln!(
                "independent governed repair CONTROLLED_SPY backend={} case={case}",
                if embedded {
                    "elephant-db"
                } else {
                    "native-surreal-memory"
                }
            );
            repair_phase(case);
            repair_phase("case.before");
            repair_case(embedded, case).await;
            repair_phase("case.complete");
        }
    }
    repair_phase("batch.complete");
}

type RepairCaseFuture = std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>;

struct RepairCase {
    fixture: Fixture,
    transport: RepairTransport,
    extractor: GovernedExtractor,
    item_id: RecordId,
    embedded: bool,
    case: &'static str,
}

// A small returned future keeps the libtest caller independent of case layout.
// Branches below have separate poll functions, as in the production dispatcher.
#[inline(never)]
fn repair_case(embedded: bool, case: &'static str) -> RepairCaseFuture {
    Box::pin(repair_case_router(embedded, case))
}

#[expect(
    clippy::future_not_send,
    clippy::significant_drop_tightening,
    reason = "The local repair runner intentionally awaits an erased future without a Send bound; Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn repair_case_router(embedded: bool, case: &'static str) {
    repair_phase("router.enter");
    if case == "unknown_usage" {
        Box::pin(unknown_usage_retains_real_reservation(embedded)).await;
        return;
    }
    let empty = json!({"assertions":[]});
    let outputs = match case {
        "first_pass_empty" => vec![empty.clone()],
        "same_live_run_replay" => vec![repaired_proposal()],
        "repeat_rejection" => vec![rejected_proposal(), rejected_proposal()],
        "empty_repair" => vec![rejected_proposal(), empty],
        _ => vec![rejected_proposal(), repaired_proposal()],
    };
    repair_phase("router.fixture.before");
    let (fixture, transport, mut extractor, item_id) = Box::pin(repair_fixture(
        embedded,
        outputs,
        case == "malformed_support_reply",
    ))
    .await;
    repair_phase("router.fixture.ready");

    if case == "token_or_time_exhausted" {
        extractor = GovernedExtractor::new(
            if embedded {
                "gpt-spy-b"
            } else {
                "claude-spy-a"
            },
            ExtractionBudget {
                max_tokens: 300,
                max_wall_time_secs: 120,
            },
        );
    }

    let context = RepairCase {
        fixture,
        transport,
        extractor,
        item_id,
        embedded,
        case,
    };
    repair_phase("router.phase.before_construct");
    let phase: RepairCaseFuture = match case {
        "first_pass_empty" => Box::pin(first_pass_empty_case(context)),
        "same_live_run_replay" => Box::pin(same_live_run_replay_case(context)),
        "retry_budget_exhausted" => Box::pin(retry_budget_exhausted_case(context)),
        "token_or_time_exhausted" => Box::pin(token_or_time_exhausted_case(context)),
        "crash_after_rejection" => Box::pin(crash_after_rejection_case(context)),
        _ => Box::pin(retry_repair_case(context)),
    };
    repair_phase("router.phase.before_poll");
    phase.await;
    repair_phase("router.phase.complete");
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn first_pass_empty_case(context: RepairCase) {
    let RepairCase {
        fixture,
        transport,
        extractor,
        item_id,
        ..
    } = context;
    let first = Box::pin(StageAttempt::claim(&fixture, &item_id, 90)).await;
    let result = Box::pin(first.execute(&fixture, &extractor)).await;
    assert!(
        result.is_ok(),
        "existing legitimate empty extraction: {result:?}"
    );
    let candidate = first.candidate(&fixture).await;
    assert_eq!(candidate.status, ArtifactStatus::Validated);
    assert_eq!(candidate.data["assertions"], json!([]));
    assert_eq!(Box::pin(first.settle(&fixture, result)).await, Ok(()));
    transport.assert_counts(1, 0);
    no_canonical(&fixture).await;
    // The real tail still has to satisfy the complete manifest and embeddings.
    Box::pin(fixture.finish_publication()).await;
    assert!(AssertionRepository::new(fixture.storage.clone())
        .list(fixture.space(), 100, None)
        .await
        .expect("legitimate empty canonical result")
        .is_empty());
    transport.assert_counts(1, 0);
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn same_live_run_replay_case(context: RepairCase) {
    let RepairCase {
        fixture,
        transport,
        extractor,
        item_id,
        ..
    } = context;
    let first = Box::pin(StageAttempt::claim(&fixture, &item_id, 90)).await;
    let result = Box::pin(first.execute(&fixture, &extractor)).await;
    assert!(result.is_ok(), "supported live attempt: {result:?}");
    let candidate = first.candidate(&fixture).await;
    assert_eq!(candidate.status, ArtifactStatus::Validated);
    let before = json!(candidate);
    let manifests =
        elephant_storage::source_publication_manifest::SourcePublicationManifestRepository::new(
            fixture.storage.clone(),
        );
    let manifest = manifests
        .get(fixture.space(), &fixture.accepted.publication)
        .await
        .expect("original manifest")
        .expect("manifest");
    assert!(!manifest.bundles.is_empty());
    let replay = Box::pin(first.execute(&fixture, &extractor)).await;
    assert!(replay.is_ok(), "same live fenced replay: {replay:?}");
    assert_eq!(json!(first.candidate(&fixture).await), before);
    assert_eq!(
        json!(
            manifests
                .get(fixture.space(), &fixture.accepted.publication)
                .await
                .expect("manifest after replay")
                .expect("manifest")
                .bundles
        ),
        json!(manifest.bundles)
    );
    transport.assert_counts(1, 1);
    assert_eq!(Box::pin(first.settle(&fixture, replay)).await, Ok(()));
    complete_known_answer(&fixture, &candidate).await;
    transport.assert_counts(1, 1);
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn retry_budget_exhausted_case(context: RepairCase) {
    let RepairCase {
        mut fixture,
        transport,
        extractor,
        item_id,
        embedded,
        ..
    } = context;
    fixture.config.transient_retry_max_attempts = 1;
    let first = Box::pin(StageAttempt::claim(&fixture, &item_id, 90)).await;
    let result = Box::pin(first.execute(&fixture, &extractor)).await;
    assert_eq!(
        classify_failure(result.as_ref().expect_err("last charged rejection")),
        FailureClass::Terminal
    );
    assert_eq!(
        Box::pin(first.settle(&fixture, result)).await,
        Err(FailureClass::Terminal)
    );
    assert_eq!(
        first.candidate(&fixture).await.status,
        ArtifactStatus::Rejected
    );
    assert_terminal(&fixture, &item_id, 1).await;
    transport.assert_counts(1, 1);
    Box::pin(preflight_terminal_and_stale_fence(embedded)).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn crash_after_rejection_case(context: RepairCase) {
    let RepairCase {
        fixture,
        transport,
        extractor,
        item_id,
        ..
    } = context;
    let first = Box::pin(StageAttempt::claim(&fixture, &item_id, 15)).await;
    let result = Box::pin(first.execute(&fixture, &extractor)).await;
    assert_eq!(
        classify_failure(
            result
                .as_ref()
                .expect_err("durably rejected before settlement")
        ),
        FailureClass::Transient
    );
    let snapshot = json!(first.candidate(&fixture).await);
    assert!(first.run(&fixture).await.completed_at.is_none());
    // Withhold run success/failure/requeue bookkeeping at the exact selected
    // boundary; use real lease expiry and abandoned-run recovery.
    expire_and_recover(&fixture, &first).await;
    elephant_pipeline::provider_usage::recover_usage(&fixture.storage, fixture.space())
        .await
        .expect("real recovery derives complete usage from original physical receipts");
    let recovered_usage = AttemptUsage::from_metrics(&first.run(&fixture).await.metrics)
        .expect("recovered usage")
        .expect("usage");
    assert_eq!(recovered_usage.basis, UsageBasis::Measured);
    assert_eq!(recovered_usage.measured_tokens, Some(300));
    let second = Box::pin(StageAttempt::claim(&fixture, &item_id, 90)).await;
    assert_eq!(second.item.attempts, 2);
    let result = Box::pin(second.execute(&fixture, &extractor)).await;
    assert!(
        result.is_ok(),
        "one complete measured interrupted proposal may repair: {result:?}"
    );
    let candidate = second.candidate(&fixture).await;
    assert_eq!(candidate.status, ArtifactStatus::Validated);
    assert_eq!(Box::pin(second.settle(&fixture, result)).await, Ok(()));
    history_unchanged(&fixture, &first, &snapshot).await;
    complete_known_answer(&fixture, &candidate).await;
    history_unchanged(&fixture, &first, &snapshot).await;
    let old_queue = WorkQueue::new(fixture.storage.clone(), WorkQueueConfig::default())
        .for_attempt(&first.item)
        .expect("old fence shape");
    assert!(
        old_queue.complete(fixture.space(), &item_id).await.is_err(),
        "expired producer cannot complete current work"
    );
    transport.assert_counts(2, 2);
}

#[expect(
    clippy::semicolon_if_nothing_returned,
    clippy::significant_drop_tightening,
    reason = "Retain the original unit-valued boxed phase tails in the repair control; Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn retry_repair_case(context: RepairCase) {
    repair_phase("retry_case.enter");
    let RepairCase {
        fixture,
        transport,
        mut extractor,
        item_id,
        embedded,
        case,
    } = context;

    let (first, rejected_snapshot) = Box::pin(rejected_first(&fixture, &extractor, &item_id)).await;
    transport.assert_counts(1, 1);
    let original_run = first.run(&fixture).await;
    let original_budget = original_run.metrics.custom[EXTRACTION_BUDGET_KEY].clone();

    match case {
        "revoked_before_retry" => {
            Box::pin(async {
                provider_spy::edit(
                    &SpaceConfigRepository::new(fixture.storage.clone()),
                    fixture.space(),
                    |processing| {
                        processing
                        .bindings
                        .get_mut(
                            &elephant_types::provider_binding::ModelOperation::SupportVerification,
                        )
                        .expect("original support binding")
                        .revoked = true;
                        Ok(())
                    },
                )
                .await
                .expect("revoke support approval before old plan retries");
            })
            .await
        }
        "wrong_space_history" | "archived_duplicate_attempt_number" => {
            Box::pin(async {
                inject_foreign_history(
                    &fixture,
                    &first,
                    case == "archived_duplicate_attempt_number",
                )
                .await;
            })
            .await
        }
        "changed_input_or_authority" => {
            Box::pin(async {
                let sources = SourceRepository::new(fixture.storage.clone());
                let mut source = sources
                    .get(fixture.space(), &fixture.accepted.source.base.id)
                    .await
                    .expect("source before mutation")
                    .expect("source");
                source.base.security.level += 1;
                sources
                    .update(fixture.space(), &source.base.id.clone(), source)
                    .await
                    .expect("controlled current source reclassification");
            })
            .await
        }
        "credential_rotation" => {
            Box::pin(async {
                let configs = SpaceConfigRepository::new(fixture.storage.clone());
                let before = configs
                    .get_by_space_id(fixture.space())
                    .await
                    .expect("before config")
                    .expect("config");
                let after = provider_spy::edit(&configs, fixture.space(), |processing| {
                    // Rotate the connection actually selected by the approved plan,
                    // including a fixture's newly approved endpoint identity.
                    let profile_id = &processing
                        .bindings
                        .get(&elephant_types::provider_binding::ModelOperation::Extraction)
                        .expect("approved extraction binding")
                        .profile_id;
                    let connection_id = processing
                        .profiles
                        .get(profile_id)
                        .expect("approved extraction profile")
                        .connection_id
                        .clone();
                    let connection = processing
                        .connections
                        .get_mut(&connection_id)
                        .expect("approved connection");
                    connection.credential_generation += 1;
                    connection.credential_ref = Some("SEMANTICS_SPY_ROTATED_GENERATE".into());
                    Ok(())
                })
                .await
                .expect("credential rotation");
                let old = before.processing.expect("before policy");
                let new = after.processing.expect("after policy");
                assert_ne!(old.generation, new.generation);
                assert_eq!(old.index_generation, new.index_generation);
                assert_eq!(
                    old.active_embedding_profile_id,
                    new.active_embedding_profile_id
                );
                assert_eq!(
                    json!(old.profiles),
                    json!(new.profiles),
                    "vector/model profile identities are unchanged by credential rotation"
                );
            })
            .await
        }
        "mutated_history_or_receipt" => {
            Box::pin(async {
                let mut changed = first.candidate(&fixture).await;
                // Copying a positive receipt cannot support a different assertion.
                changed.data["assertions"][2]["support_verification"] =
                    changed.data["assertions"][0]["support_verification"].clone();
                WorkArtifactRepository::new(fixture.storage.clone())
                    .update(fixture.space(), &changed.base.id.clone(), changed)
                    .await
                    .expect("controlled receipt transplantation");
            })
            .await
        }
        "remaining_budget_decreases" => {
            Box::pin(async {
                // New extractor instance models process-local configuration reload.
                // Its tighter bound is separate from the immutable original allowance.
                extractor = GovernedExtractor::new(
                    if embedded {
                        "gpt-spy-b"
                    } else {
                        "claude-spy-a"
                    },
                    ExtractionBudget {
                        max_tokens: 700,
                        max_wall_time_secs: 60,
                    },
                );
            })
            .await
        }
        _ => Box::pin(async {}).await,
    }

    let second = Box::pin(StageAttempt::claim(
        &fixture,
        &item_id,
        if case == "expired_second_repair_producer" {
            15
        } else {
            90
        },
    ))
    .await;
    assert_eq!(second.item.attempts, 2);
    assert_ne!(second.run_id, first.run_id);
    assert_ne!(second.item.leased_by, first.item.leased_by);
    let result = Box::pin(second.execute(&fixture, &extractor)).await;

    match case {
        "repair_success"
        | "remaining_budget_decreases"
        | "erasure_with_repair_history"
        | "completed_producer_validator_crash" => {
            Box::pin(async {
                assert!(
                    result.is_ok(),
                    "complete independently verified repair failed: {result:?}"
                );
                let candidate = second.candidate(&fixture).await;
                assert_eq!(candidate.status, ArtifactStatus::Validated);
                assert_eq!(
                    candidate.data["assertions"]
                        .as_array()
                        .expect("complete repaired proposal")
                        .len(),
                    2
                );
                assert!(candidate.data["assertions"]
                    .as_array()
                    .expect("claims")
                    .iter()
                    .all(|claim| claim["support_verification"].is_object()));
                assert_ne!(candidate.base.id, first.candidate(&fixture).await.base.id);
                let snapshot =
                    second.run(&fixture).await.metrics.custom[EXTRACTION_BUDGET_KEY].clone();
                assert_eq!(
                    snapshot["original"], original_budget["original"],
                    "original hard budget/start/input never reset"
                );
                let observed = extractor
                    .contexts
                    .lock()
                    .expect("observed contexts")
                    .clone();
                let (budget, repairing) = observed.last().expect("repair context");
                assert!(*repairing);
                assert!(budget.max_tokens <= 3_700 && budget.max_wall_time_secs < 120);
                if case == "remaining_budget_decreases" {
                    assert_eq!(
                        observed.len(),
                        1,
                        "new process-local extractor observed only the repair"
                    );
                    assert!(budget.max_tokens <= 700 && budget.max_wall_time_secs <= 60);
                }
                assert_eq!(Box::pin(second.settle(&fixture, result)).await, Ok(()));
                history_unchanged(&fixture, &first, &rejected_snapshot).await;
                transport.assert_counts(2, 2);
                if case == "completed_producer_validator_crash" {
                    let validated = Box::pin(
                        elephant_pipeline::scheduler::staged_commit::validate_artifacts::execute(
                            &StagedCommit::new(fixture.storage.clone()),
                            fixture.space(),
                            &second.run_id,
                        ),
                    )
                    .await
                    .expect("actual validator durable boundary before bundle creation");
                    assert_eq!(validated.len(), 1);
                    assert_eq!(validated[0].status, ArtifactStatus::Validated);
                    no_bundle(&fixture, &second.run_id).await;
                    // Producer is already durably DONE; replay of this downstream
                    // validation boundary rechecks b537 and must make no model call.
                    let done = WorkItemRepository::new(fixture.storage.clone())
                        .get(fixture.space(), &item_id)
                        .await
                        .expect("producer")
                        .expect("producer");
                    assert_eq!(done.status, WorkItemStatus::Done);
                    assert!(second.run(&fixture).await.success);
                }
                complete_known_answer(&fixture, &candidate).await;
                history_unchanged(&fixture, &first, &rejected_snapshot).await;
                transport.assert_counts(2, 2);
                if case == "erasure_with_repair_history" {
                    Box::pin(erasure_and_restore(&fixture, &first, &second)).await;
                    transport.assert_counts(2, 2);
                }
            })
            .await
        }
        "repeat_rejection" | "empty_repair" | "malformed_support_reply" => {
            Box::pin(async {
                assert!(
                    result.is_err(),
                    "failed repair cannot be treated as producer success"
                );
                let _class = Box::pin(second.settle(&fixture, result))
                    .await
                    .expect_err("failed repair");
                assert_terminal(&fixture, &item_id, 2).await;
                history_unchanged(&fixture, &first, &rejected_snapshot).await;
                no_bundle(&fixture, &second.run_id).await;
                if case == "repeat_rejection" {
                    assert_eq!(
                        second.candidate(&fixture).await.status,
                        ArtifactStatus::Rejected
                    );
                }
                if case == "malformed_support_reply" {
                    let artifacts = WorkArtifactRepository::new(fixture.storage.clone())
                        .find_by_work_run(&second.run_id)
                        .await
                        .expect("malformed support result");
                    assert!(artifacts
                        .iter()
                        .all(|a| a.status != ArtifactStatus::Validated));
                    assert!(artifacts
                        .iter()
                        .all(
                            |a| a.data["assertions"].as_array().is_none_or(|claims| claims
                                .iter()
                                .all(|c| c["support_verification"].is_null()))
                        ));
                }
                transport.assert_counts(2, if case == "empty_repair" { 1 } else { 2 });
            })
            .await
        }
        "expired_second_repair_producer" => {
            Box::pin(async {
                assert!(result.is_ok(), "validated repair boundary: {result:?}");
                let candidate = second.candidate(&fixture).await;
                assert_eq!(candidate.status, ArtifactStatus::Validated);
                let second_snapshot = json!(candidate);
                assert!(second.run(&fixture).await.completed_at.is_none());
                expire_and_recover(&fixture, &second).await;
                elephant_pipeline::provider_usage::recover_usage(&fixture.storage, fixture.space())
                    .await
                    .expect("real recovery accounts the completed second physical batch");
                let recovered_usage =
                    AttemptUsage::from_metrics(&second.run(&fixture).await.metrics)
                        .expect("complete recovered second usage")
                        .expect("usage");
                assert_eq!(recovered_usage.basis, UsageBasis::Measured);
                assert_eq!(recovered_usage.measured_tokens, Some(300));
                assert_eq!(attempt_ledger(&fixture, &second).await.total_runs, 1);
                let third = Box::pin(StageAttempt::claim(&fixture, &item_id, 90)).await;
                assert_eq!(
                    third.item.attempts, 3,
                    "new recovery generation is not free semantic retry"
                );
                let result = Box::pin(third.execute(&fixture, &extractor)).await;
                assert_eq!(
                    classify_failure(result.as_ref().expect_err("no cross-run validated handoff")),
                    FailureClass::Terminal
                );
                assert_eq!(
                    Box::pin(third.settle(&fixture, result)).await,
                    Err(FailureClass::Terminal)
                );
                assert_terminal(&fixture, &item_id, 3).await;
                assert_eq!(json!(second.candidate(&fixture).await), second_snapshot);
                assert!(!second.run(&fixture).await.success);
                history_unchanged(&fixture, &first, &rejected_snapshot).await;
                transport.assert_counts(2, 2);
            })
            .await
        }
        _ => {
            Box::pin(async {
                assert!(
                    result.is_err(),
                    "changed/incomplete authority must refuse before outbound call"
                );
                let _class = Box::pin(second.settle(&fixture, result))
                    .await
                    .expect_err("refused repair");
                no_canonical(&fixture).await;
                no_bundle(&fixture, &second.run_id).await;
                transport.assert_counts(1, 1);
                if case != "mutated_history_or_receipt" {
                    history_unchanged(&fixture, &first, &rejected_snapshot).await;
                }
                if matches!(
                    case,
                    "wrong_space_history"
                        | "archived_duplicate_attempt_number"
                        | "mutated_history_or_receipt"
                ) {
                    assert_terminal(&fixture, &item_id, 2).await;
                }
                assert_eq!(
                    first.run(&fixture).await.metrics.custom[EXTRACTION_BUDGET_KEY],
                    original_budget
                );
                if matches!(case, "revoked_before_retry" | "credential_rotation") {
                    let old = WorkItemRepository::new(fixture.storage.clone())
                        .get(fixture.space(), &item_id)
                        .await
                        .expect("old plan retained")
                        .expect("old work");
                    assert_eq!(
                        old.payload["_source_configuration"],
                        first.item.payload["_source_configuration"]
                    );
                    assert_eq!(
                        old.payload["_source_plan"],
                        first.item.payload["_source_plan"]
                    );
                }
            })
            .await
        }
    }
    if case == "archived_duplicate_attempt_number" {
        Box::pin(consumed_repair_configuration_failure_cannot_refund(
            embedded,
        ))
        .await;
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn token_or_time_exhausted_case(context: RepairCase) {
    let RepairCase {
        fixture,
        transport,
        extractor,
        item_id,
        embedded,
        ..
    } = context;
    // The SDK stops at used >= limit. Extraction spends 150 tokens, leaving
    // exactly 150 for support. That second response exhausts the original 300;
    // it cannot finish verification and become a rejected proposal for repair.
    let first = Box::pin(StageAttempt::claim(&fixture, &item_id, 90)).await;
    let result = Box::pin(first.execute(&fixture, &extractor)).await;
    let error = result
        .as_ref()
        .expect_err("original token allowance exhausted");
    assert!(
        error.chain().any(|cause| matches!(
            cause.downcast_ref::<PipelineError>(),
            Some(PipelineError::ExtractionError(message))
                if message == "Extraction agent terminated: BudgetExhausted"
        )),
        "the actual SDK budget terminal must cause this refusal: {error:#}"
    );
    assert_eq!(classify_failure(error), FailureClass::Terminal);
    transport.assert_counts(1, 1);
    let physical = transport.calls();
    let support_request = &physical.last().expect("actual support request").body;
    assert_eq!(
        support_request[if embedded {
            "max_output_tokens"
        } else {
            "max_tokens"
        }],
        150,
        "the support request receives only the original remaining tokens"
    );
    let original_budget = first.run(&fixture).await.metrics.custom[EXTRACTION_BUDGET_KEY].clone();
    assert_eq!(original_budget["original"]["budget"]["max_tokens"], 300);
    assert_eq!(
        original_budget["original"]["budget"]["max_wall_time_secs"],
        120
    );
    assert!(WorkArtifactRepository::new(fixture.storage.clone())
        .find_by_work_run(&first.run_id)
        .await
        .expect("budget-terminated producer artifacts")
        .is_empty());
    assert_eq!(
        Box::pin(first.settle(&fixture, result)).await,
        Err(FailureClass::Terminal)
    );
    let run = first.run(&fixture).await;
    assert!(!run.success && run.completed_at.is_some());
    assert_eq!(run.metrics.custom[EXTRACTION_BUDGET_KEY], original_budget);
    let usage = AttemptUsage::from_metrics(&run.metrics)
        .expect("typed exhausted usage")
        .expect("measured usage retained");
    assert_eq!(usage.basis, UsageBasis::Measured);
    assert_eq!(usage.measured_tokens, Some(300));
    let receipts = ProviderUsageRepository::new(fixture.storage.clone())
        .for_scope(fixture.space(), &first.run_id.to_string())
        .await
        .expect("actual exhausted physical receipts");
    let fence = elephant_storage::work_attempt_repository::fence_from_claim(&first.item)
        .expect("original exhausted fence");
    assert_eq!(receipts.len(), 2);
    assert!(receipts.iter().all(|receipt| receipt.usage_complete
        && receipt.attempt.as_ref() == Some(&fence)
        && receipt.input_tokens == Some(100)
        && receipt.output_tokens == Some(50)));
    assert_terminal(&fixture, &item_id, 1).await;
    no_bundle(&fixture, &first.run_id).await;
    assert_eq!(
        WorkRunRepository::new(fixture.storage.clone())
            .find_by_work_item(&item_id)
            .await
            .expect("no additional exhausted run")
            .len(),
        1,
        "terminal token exhaustion cannot authorize a repair attempt"
    );
    transport.assert_counts(1, 1);

    // Keep the independent retained-rejection boundaries: a real original
    // deadline expires before attempt two, and an incomplete physical repair
    // receipt refuses the SDK's next request. Neither rewrites old limits/usage.
    Box::pin(original_deadline_exhaustion(embedded)).await;
    Box::pin(physical_retry_guard_refusal(embedded)).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn original_deadline_exhaustion(embedded: bool) {
    let (fixture, transport, _, item_id) = Box::pin(repair_fixture(
        embedded,
        vec![rejected_proposal(), repaired_proposal()],
        false,
    ))
    .await;
    let extractor = GovernedExtractor::new(
        if embedded {
            "gpt-spy-b"
        } else {
            "claude-spy-a"
        },
        ExtractionBudget {
            max_tokens: 4_000,
            max_wall_time_secs: 10,
        },
    );
    let (first, snapshot) = Box::pin(rejected_first(&fixture, &extractor, &item_id)).await;
    let original = first.run(&fixture).await.metrics.custom[EXTRACTION_BUDGET_KEY].clone();
    let deadline: chrono::DateTime<Utc> =
        serde_json::from_value(original["original"]["deadline"].clone())
            .expect("actual original effective deadline");
    let remaining = deadline
        .signed_duration_since(Utc::now())
        .num_milliseconds();
    assert!(
        (0..=10_000).contains(&remaining),
        "first request must fit its explicit original allowance"
    );
    tokio::time::sleep(std::time::Duration::from_millis(
        u64::try_from(remaining).expect("bounded real time") + 5,
    ))
    .await;
    let second = Box::pin(StageAttempt::claim(&fixture, &item_id, 90)).await;
    let result = Box::pin(second.execute(&fixture, &extractor)).await;
    assert_eq!(
        classify_failure(result.as_ref().expect_err("expired original deadline")),
        FailureClass::Terminal
    );
    assert_eq!(
        Box::pin(second.settle(&fixture, result)).await,
        Err(FailureClass::Terminal)
    );
    assert_terminal(&fixture, &item_id, 2).await;
    history_unchanged(&fixture, &first, &snapshot).await;
    assert_eq!(
        first.run(&fixture).await.metrics.custom[EXTRACTION_BUDGET_KEY],
        original
    );
    transport.assert_counts(1, 1);
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn physical_retry_guard_refusal(embedded: bool) {
    let (fixture, transport, extractor, item_id) = Box::pin(repair_fixture(
        embedded,
        vec![rejected_proposal(), repaired_proposal()],
        false,
    ))
    .await;
    let (first, snapshot) = Box::pin(rejected_first(&fixture, &extractor, &item_id)).await;
    transport
        .script
        .lock()
        .expect("controlled second HTTP failure")
        .fail_second_extraction = true;
    let second = Box::pin(StageAttempt::claim(&fixture, &item_id, 90)).await;
    let result = Box::pin(second.execute(&fixture, &extractor)).await;
    assert_eq!(
        classify_failure(
            result
                .as_ref()
                .expect_err("incomplete receipt must block SDK retry")
        ),
        FailureClass::Terminal,
        "typed physical-dispatch refusal must survive the real SDK/adapters"
    );
    assert!(format!("{:#}", result.as_ref().expect_err("actual guarded SDK retry"))
        .contains("recall_dispatch_terminal:"),
        "the production callback marker must be present; a generic HTTP500 error with no retry is not this control");
    assert_eq!(
        Box::pin(second.settle(&fixture, result)).await,
        Err(FailureClass::Terminal)
    );
    assert_terminal(&fixture, &item_id, 2).await;
    history_unchanged(&fixture, &first, &snapshot).await;
    transport.assert_counts(2, 1);
    let usage = AttemptUsage::from_metrics(&second.run(&fixture).await.metrics)
        .expect("actual refused-attempt usage")
        .expect("usage");
    assert_eq!(usage.basis, UsageBasis::Unknown);
    assert_eq!(usage.reserved_tokens, 1_000);
    let ledger = attempt_ledger(&fixture, &second).await;
    assert_eq!(
        ledger.total_runs, 0,
        "unknown physical repair is not a settled reservation"
    );
    assert_eq!(
        ledger.reserved_euros, 7.0,
        "original actual reservation stays open"
    );
    let physical = ProviderUsageRepository::new(fixture.storage.clone())
        .for_scope(fixture.space(), &second.run_id.to_string())
        .await
        .expect("actual second physical receipt");
    assert_eq!(
        physical.len(),
        1,
        "refused SDK retry must not invent another dispatched receipt"
    );
    assert!(!physical[0].usage_complete);
    assert_eq!(physical[0].input_tokens, None);
    assert_eq!(physical[0].output_tokens, None);
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn preflight_terminal_and_stale_fence(embedded: bool) {
    let (fixture, transport, extractor, item_id) = Box::pin(repair_fixture(
        embedded,
        vec![rejected_proposal(), repaired_proposal()],
        false,
    ))
    .await;
    let (first, snapshot) = Box::pin(rejected_first(&fixture, &extractor, &item_id)).await;
    let repository = WorkItemRepository::new(fixture.storage.clone());
    let queued = repository
        .get(fixture.space(), &item_id)
        .await
        .expect("queued")
        .expect("original work");
    let remaining = queued
        .run_after
        .expect("real backoff")
        .signed_duration_since(Utc::now())
        .num_milliseconds();
    assert!(remaining <= 1_000);
    if remaining > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(
            u64::try_from(remaining).expect("real due delay") + 5,
        ))
        .await;
    }
    // Production preflight runs under a claim before constructing a WorkRun.
    let claimed = repository
        .claim_with_lease(
            fixture.space(),
            &item_id,
            "preflight-current",
            Utc::now() + chrono::Duration::seconds(90),
        )
        .await
        .expect("current preflight claim")
        .expect("due");
    assert_eq!(claimed.attempts, 2);
    let before_item = json!(repository
        .get(fixture.space(), &item_id)
        .await
        .expect("before item"));
    let before_operation = json!(operation(&fixture).await);
    let failure: anyhow::Error =
        elephant_pipeline::scheduler::execution_contract::WorkFailure::timeout(1).into();
    let _stale_result = source_runtime::record_preflight_failure(
        &fixture.storage,
        &first.item,
        &failure,
        &fixture.config,
    )
    .await;
    assert_eq!(
        json!(repository
            .get(fixture.space(), &item_id)
            .await
            .expect("after stale writer")),
        before_item
    );
    assert_eq!(
        json!(operation(&fixture).await),
        before_operation,
        "stale original fence cannot alter current source operation"
    );
    source_runtime::record_preflight_failure(&fixture.storage, &claimed, &failure, &fixture.config)
        .await
        .expect("actual current preflight source-first disposition");
    assert_terminal(&fixture, &item_id, 2).await;
    let runs = WorkRunRepository::new(fixture.storage.clone())
        .find_by_work_item(&item_id)
        .await
        .expect("preflight history");
    assert_eq!(
        runs.len(),
        1,
        "preflight must not fabricate an execution/usage WorkRun"
    );
    assert_eq!(runs[0].base.id, first.run_id);
    history_unchanged(&fixture, &first, &snapshot).await;
    transport.assert_counts(1, 1);
}

async fn attempt_ledger(
    fixture: &Fixture,
    attempt: &StageAttempt,
) -> elephant_types::records::ingest::CostLedgerDaily {
    let run = attempt.run(fixture).await;
    let id = RecordId::from_str(
        run.metrics.custom["cost_ledger_id"]
            .as_str()
            .expect("original real cost reservation ID"),
    )
    .expect("ledger ID");
    elephant_storage::cost_ledger_daily_repository::CostLedgerDailyRepository::new(
        fixture.storage.clone(),
    )
    .get(fixture.space(), &id)
    .await
    .expect("original durable ledger")
    .expect("reservation exists")
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn unknown_usage_retains_real_reservation(embedded: bool) {
    let (fixture, transport, extractor, item_id) = Box::pin(repair_fixture(
        embedded,
        vec![rejected_proposal(), repaired_proposal()],
        false,
    ))
    .await;
    let first = Box::pin(StageAttempt::claim(&fixture, &item_id, 90)).await;
    let reserved = attempt_ledger(&fixture, &first).await;
    assert_eq!(reserved.reserved_euros, 7.0);
    assert_eq!(reserved.total_runs, 0);
    let result = Box::pin(first.execute(&fixture, &extractor)).await;
    assert_eq!(
        classify_failure(result.as_ref().expect_err("whole-batch rejection")),
        FailureClass::Transient
    );
    let candidate = first.candidate(&fixture).await;
    assert_eq!(candidate.status, ArtifactStatus::Rejected);
    let snapshot = json!(candidate);
    let original_budget = first.run(&fixture).await.metrics.custom[EXTRACTION_BUDGET_KEY].clone();
    let repository = ProviderUsageRepository::new(fixture.storage.clone());
    let mut receipts = repository
        .for_scope(fixture.space(), &first.run_id.to_string())
        .await
        .expect("original physical receipts");
    assert_eq!(receipts.len(), 2);
    // Model the missing final durable observation BEFORE any settlement, so the
    // actual reservation is still open. Never rewrite an already settled ledger.
    receipts[1].output_tokens = None;
    receipts[1].usage_complete = false;
    overwrite_partial_receipt(&fixture, &receipts[1]).await;
    assert_eq!(
        Box::pin(first.settle(&fixture, result)).await,
        Err(FailureClass::Transient)
    );
    let usage = AttemptUsage::from_metrics(&first.run(&fixture).await.metrics)
        .expect("partial physical usage")
        .expect("usage");
    assert_eq!(usage.basis, UsageBasis::Unknown);
    assert_eq!(usage.measured_tokens, Some(250));
    assert_eq!(usage.reserved_tokens, 1_000);
    assert_eq!(usage.reserved_budget_units, 7.0);
    assert_eq!(
        json!(attempt_ledger(&fixture, &first).await),
        json!(reserved),
        "reconciliation Ok must retain the exact original reservation when usage is incomplete"
    );
    elephant_pipeline::provider_usage::reconcile_finished_usage(
        &fixture.storage,
        fixture.space(),
        &first.run_id,
    )
    .await
    .expect("idempotent incomplete settlement attempt");
    let second = Box::pin(StageAttempt::claim(&fixture, &item_id, 90)).await;
    let result = Box::pin(second.execute(&fixture, &extractor)).await;
    assert_eq!(
        classify_failure(
            result
                .as_ref()
                .expect_err("unknown liability cannot admit extra call")
        ),
        FailureClass::Terminal
    );
    assert_eq!(
        Box::pin(second.settle(&fixture, result)).await,
        Err(FailureClass::Terminal)
    );
    assert_terminal(&fixture, &item_id, 2).await;
    assert_eq!(
        json!(attempt_ledger(&fixture, &first).await),
        json!(reserved)
    );
    let unused = attempt_ledger(&fixture, &second).await;
    assert_eq!(
        unused.total_runs, 1,
        "provable NotStarted second attempt settles only its own reservation"
    );
    assert_eq!(unused.total_tokens, 0);
    assert_eq!(unused.total_euros, 0.0);
    assert_eq!(
        first.run(&fixture).await.metrics.custom[EXTRACTION_BUDGET_KEY],
        original_budget
    );
    history_unchanged(&fixture, &first, &snapshot).await;
    transport.assert_counts(1, 1);
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn consumed_repair_configuration_failure_cannot_refund(embedded: bool) {
    let (fixture, transport, mut extractor, item_id) = Box::pin(repair_fixture(
        embedded,
        vec![rejected_proposal(), repaired_proposal()],
        false,
    ))
    .await;
    let (first, snapshot) = Box::pin(rejected_first(&fixture, &extractor, &item_id)).await;
    let original_configuration = first.item.payload["_source_configuration"].clone();
    extractor.block_completed_repair = true;
    let second = Box::pin(StageAttempt::claim(&fixture, &item_id, 90)).await;
    assert_eq!(second.item.attempts, 2);
    assert_eq!(
        second.item.payload["_source_configuration"],
        original_configuration
    );
    let result = Box::pin(second.execute(&fixture, &extractor)).await;
    assert_eq!(classify_failure(result.as_ref().expect_err("selected post-response boundary")), FailureClass::ConfigurationBlocked,
        "retain the actual cause while independently selecting terminal disposition for consumed repair");
    transport.assert_counts(2, 2);
    let usage = second.usage().await;
    assert_eq!(usage.basis, UsageBasis::Measured);
    assert_eq!(
        usage.measured_tokens,
        Some(300),
        "both repair calls physically completed"
    );
    let receipts = ProviderUsageRepository::new(fixture.storage.clone())
        .for_scope(fixture.space(), &second.run_id.to_string())
        .await
        .expect("complete repair physical receipts");
    assert_eq!(receipts.len(), 2);
    assert!(receipts
        .iter()
        .all(|r| r.usage_complete && r.input_tokens == Some(100) && r.output_tokens == Some(50)));
    assert!(
        WorkArtifactRepository::new(fixture.storage.clone())
            .find_by_work_run(&second.run_id)
            .await
            .expect("pre-artifact interruption")
            .is_empty(),
        "the control must exercise consumed physical repair without a second proposal artifact"
    );
    assert_eq!(
        Box::pin(second.settle(&fixture, result)).await,
        Err(FailureClass::ConfigurationBlocked)
    );
    assert_terminal(&fixture, &item_id, 2).await;
    let after = WorkItemRepository::new(fixture.storage.clone())
        .get(fixture.space(), &item_id)
        .await
        .expect("original charged item")
        .expect("item");
    assert_eq!(
        after.payload["_source_configuration"], original_configuration,
        "no fresh token or synthetic replan is used to excuse the consumed original attempt"
    );
    assert_eq!(attempt_ledger(&fixture, &second).await.total_runs, 1);
    history_unchanged(&fixture, &first, &snapshot).await;
    transport.assert_counts(2, 2);
}
