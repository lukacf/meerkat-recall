//! Independent known answers through the actual SDK and dispatch observer.
//! Fixed vectors establish byte coverage and arithmetic, never model quality.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;
use crate::governed::{
    CredentialResolver, EmbeddingBindingResolver, EmbeddingDispatch, ProviderDispatchObserver,
};
use elephant_types::provider_binding::{
    ModelBindingProvenance, ModelOperation, ModelProfile, ProviderConnection, ResolvedModelBinding,
    SpaceConfigToken,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const LITERALS: &str =
    include_str!("../../../tests/recall_preview/embedding-full-input-fixtures.json");
const LITERALS_SHA: &str = "a5ff1331370d02e0dbb9be58257ba425d5cb12e47e571a1458347dbfbccfa828";
const DEADLINE: Duration = Duration::from_secs(8);
const MAX_PHYSICAL_CALLS: usize = 64;
const MAX_REQUEST_BYTES: usize = 1_048_576;
const MAX_CAPTURE_BYTES: usize = 8_388_608;

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[derive(Deserialize)]
struct Part {
    text: String,
    repeat: usize,
}
fn input(parts: &[Part]) -> String {
    parts
        .iter()
        .map(|part| part.text.repeat(part.repeat))
        .collect()
}
#[derive(Deserialize)]
struct Span {
    start: usize,
    end: usize,
    utf8_bytes: usize,
    sha256: String,
}
#[derive(Deserialize)]
struct SegmentationCase {
    id: String,
    input_parts: Vec<Part>,
    input_sha256: String,
    expected_segments: Vec<Span>,
}
#[derive(Deserialize)]
struct AggregateCase {
    id: String,
    input_parts: Vec<Part>,
    segment_utf8_bytes: Vec<usize>,
    vectors: Vec<Vec<f32>>,
    expected_unit_vector: Vec<f64>,
}
#[derive(Deserialize)]
struct LargeCase {
    input_parts: Vec<Part>,
    input_sha256: String,
    expected_segment_count: usize,
    expected_final_segment: Span,
    sdk_expected_unit_vector: Vec<f64>,
}
#[derive(Deserialize)]
struct Fixtures {
    segmentation_cases: Vec<SegmentationCase>,
    aggregation_cases: Vec<AggregateCase>,
    original_full_workload_capacity: LargeCase,
}
fn fixtures() -> Fixtures {
    assert_eq!(
        digest(LITERALS.as_bytes()),
        LITERALS_SHA,
        "independent literal source changed"
    );
    serde_json::from_str(LITERALS).expect("independently frozen literal fixture")
}
fn close(actual: &[f32], expected: &[f64]) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert!(
            actual.is_finite() && (f64::from(*actual) - expected).abs() <= 1e-7,
            "{actual} differs from literal {expected}"
        );
    }
}

#[test]
fn independent_full_input_spans_and_original_two_million_byte_suffix() {
    let data = fixtures();
    for case in data.segmentation_cases {
        let original = input(&case.input_parts);
        assert_eq!(digest(original.as_bytes()), case.input_sha256);
        let segments = full_input::segments(std::slice::from_ref(&original));
        assert_eq!(segments.len(), case.expected_segments.len(), "{}", case.id);
        for (actual, expected) in segments.iter().zip(case.expected_segments) {
            assert_eq!(
                (actual.input, actual.start, actual.end),
                (0, expected.start, expected.end),
                "{}",
                case.id
            );
            assert_eq!(actual.text.len(), expected.utf8_bytes);
            assert_eq!(digest(actual.text.as_bytes()), expected.sha256);
        }
        assert_eq!(
            segments
                .iter()
                .map(|part| part.text.as_str())
                .collect::<String>(),
            original
        );
    }
    let large = data.original_full_workload_capacity;
    let original = input(&large.input_parts);
    assert_eq!(original.len(), 2_000_000);
    assert_eq!(digest(original.as_bytes()), large.input_sha256);
    let segments = full_input::segments(std::slice::from_ref(&original));
    assert_eq!(segments.len(), 977);
    assert_eq!(segments.len(), large.expected_segment_count);
    for (index, segment) in segments[..976].iter().enumerate() {
        assert_eq!(
            (segment.input, segment.start, segment.end),
            (0, index * 2048, (index + 1) * 2048)
        );
    }
    let last = segments.last().unwrap();
    assert_eq!(
        (last.start, last.end, last.text.len()),
        (1_998_848, 2_000_000, 1152)
    );
    assert_eq!(
        digest(last.text.as_bytes()),
        large.expected_final_segment.sha256
    );
    assert_eq!(
        segments
            .iter()
            .map(|part| part.text.as_str())
            .collect::<String>(),
        original
    );
}

#[test]
fn independent_full_input_weighted_vectors_suffix_and_input_order() {
    let cases = fixtures().aggregation_cases;
    let texts: Vec<_> = cases.iter().map(|case| input(&case.input_parts)).collect();
    let segments = full_input::segments(&texts);
    let mut accumulator = full_input::Accumulator::new(&texts, 2).unwrap();
    let mut ordinals = vec![0; cases.len()];
    for segment in segments {
        let case = &cases[segment.input];
        let ordinal = ordinals[segment.input];
        assert_eq!(
            segment.text.len(),
            case.segment_utf8_bytes[ordinal],
            "{}",
            case.id
        );
        accumulator.add(&segment, &case.vectors[ordinal]).unwrap();
        ordinals[segment.input] += 1;
    }
    let results = accumulator.finish().unwrap();
    assert_eq!(results.len(), cases.len());
    for (actual, expected) in results.iter().zip(&cases) {
        close(actual, &expected.expected_unit_vector);
    }
    assert_ne!(results[0], results[1], "different suffixes must contribute");
}

#[test]
fn independent_full_input_refuses_incomplete_duplicate_shape_and_nonfinite_parts() {
    let texts = vec!["a".repeat(2048) + "Z"];
    let parts = full_input::segments(&texts);
    let mut partial = full_input::Accumulator::new(&texts, 2).unwrap();
    partial.add(&parts[0], &[1.0, 0.0]).unwrap();
    assert!(partial.finish().is_err());
    let mut duplicate = full_input::Accumulator::new(&texts, 2).unwrap();
    duplicate.add(&parts[0], &[1.0, 0.0]).unwrap();
    assert!(duplicate.add(&parts[0], &[1.0, 0.0]).is_err());
    for vector in [
        vec![1.0],
        vec![0.0, 0.0],
        vec![f32::NAN, 0.0],
        vec![f32::INFINITY, 0.0],
        vec![f32::NEG_INFINITY, 0.0],
    ] {
        let mut aggregate = full_input::Accumulator::new(&texts, 2).unwrap();
        assert!(aggregate.add(&parts[0], &vector).is_err());
    }
    let zero_text = vec!["a".repeat(4096)];
    let parts = full_input::segments(&zero_text);
    let mut zero = full_input::Accumulator::new(&zero_text, 2).unwrap();
    zero.add(&parts[0], &[1.0, 0.0]).unwrap();
    zero.add(&parts[1], &[-1.0, 0.0]).unwrap();
    assert!(zero.finish().is_err());
    assert!(full_input::segments(&["🦊".into()])
        .remove(0)
        .split()
        .is_err());
}

type Reply = dyn Fn(usize, &[String]) -> (u16, Value) + Send + Sync;
struct Server {
    endpoint: String,
    calls: Arc<Mutex<Vec<Vec<String>>>>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), String>>>,
}
impl Server {
    async fn start(reply: Arc<Reply>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let captured = calls.clone();
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut total_bytes = 0usize;
            loop {
                let accepted = tokio::select! {
                    _ = &mut stopped => return Ok(()),
                    accepted = listener.accept() => accepted.map_err(|error| error.to_string())?,
                };
                let (mut socket, _) = accepted;
                let body = tokio::time::timeout(DEADLINE, Self::read_request(&mut socket))
                    .await
                    .map_err(|_| "original read deadline".to_owned())??;
                total_bytes += body.len();
                if total_bytes > MAX_CAPTURE_BYTES {
                    return Err("original capture cap".into());
                }
                let value: Value =
                    serde_json::from_slice(&body).map_err(|error| error.to_string())?;
                if value["model"] != "independent-full-input" || value["dimensions"] != 2 {
                    return Err("original model/shape changed".into());
                }
                let inputs: Vec<String> = serde_json::from_value(value["input"].clone())
                    .map_err(|error| error.to_string())?;
                let ordinal = {
                    let mut calls = captured.lock().unwrap();
                    if calls.len() >= MAX_PHYSICAL_CALLS {
                        return Err("original physical call cap".into());
                    }
                    calls.push(inputs.clone());
                    calls.len() - 1
                };
                let (status, response) = reply(ordinal, &inputs);
                let response = serde_json::to_vec(&response).map_err(|error| error.to_string())?;
                let headers = format!("HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", response.len());
                tokio::time::timeout(DEADLINE, async {
                    socket.write_all(headers.as_bytes()).await?;
                    socket.write_all(&response).await?;
                    socket.shutdown().await
                })
                .await
                .map_err(|_| "original write deadline".to_owned())?
                .map_err(|error| error.to_string())?;
            }
        });
        Self {
            endpoint,
            calls,
            stop: Some(stop),
            task: Some(task),
        }
    }
    async fn read_request(socket: &mut tokio::net::TcpStream) -> Result<Vec<u8>, String> {
        let mut bytes = Vec::new();
        loop {
            let mut chunk = [0_u8; 4096];
            let read = socket
                .read(&mut chunk)
                .await
                .map_err(|error| error.to_string())?;
            if read == 0 {
                return Err("incomplete original HTTP body".into());
            }
            bytes.extend_from_slice(&chunk[..read]);
            if bytes.len() > MAX_REQUEST_BYTES {
                return Err("original request byte cap".into());
            }
            let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") else {
                continue;
            };
            let headers = std::str::from_utf8(&bytes[..end]).map_err(|error| error.to_string())?;
            if !headers.starts_with("POST /v1/embeddings HTTP/1.1\r\n") {
                return Err("unexpected original request".into());
            }
            if !headers.lines().any(|line| {
                line.split_once(':').is_some_and(|(key, value)| {
                    key.eq_ignore_ascii_case("authorization")
                        && value.trim() == "Bearer independent-synthetic-key"
                })
            }) {
                return Err("original synthetic credential changed".into());
            }
            let length: usize = headers
                .lines()
                .find_map(|line| {
                    line.split_once(':')
                        .filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                        .map(|(_, value)| value.trim().parse::<usize>())
                })
                .ok_or("missing original length")?
                .map_err(|error| error.to_string())?;
            if length > MAX_REQUEST_BYTES {
                return Err("declared body cap".into());
            }
            if bytes.len() >= end + 4 + length {
                if bytes.len() != end + 4 + length {
                    return Err("unexpected trailing request bytes".into());
                }
                break Ok::<_, String>(bytes[end + 4..].to_vec());
            }
        }
    }

    async fn finish(mut self) -> Vec<Vec<String>> {
        self.stop.take().unwrap().send(()).ok();
        tokio::time::timeout(DEADLINE, self.task.as_mut().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        self.task.take();
        let calls = self.calls.lock().unwrap().clone();
        calls
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            stop.send(()).ok();
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
fn success(vectors: Vec<Vec<f32>>) -> (u16, Value) {
    (
        200,
        json!({"data":vectors.into_iter().enumerate().map(|(index,embedding)|json!({"index":index,"embedding":embedding})).collect::<Vec<_>>(),
        "model":"independent-full-input","usage":{"prompt_tokens":7,"total_tokens":7}}),
    )
}
fn size_error(index: usize) -> (u16, Value) {
    (
        400,
        json!({"error":{"message":format!("Invalid 'input[{index}]': maximum input length"),"type":"invalid_request_error"}}),
    )
}
fn provider(server: &Server, preprocessing: &str, batch: usize) -> OpenAIEmbeddingProvider {
    let mut config = OpenAIConfig::new("independent-synthetic-key")
        .with_base_url(&server.endpoint)
        .with_model("independent-full-input")
        .with_dimensions(2)
        .with_preprocessing(preprocessing);
    config.max_batch_size = batch;
    config.max_retries = 1;
    OpenAIEmbeddingProvider::new(config)
}

#[tokio::test]
async fn sdk_full_input_uses_literal_suffix_vectors_and_keeps_original_input_order() {
    let cases = fixtures().aggregation_cases;
    let mut declared = BTreeMap::new();
    let texts: Vec<_> = cases.iter().map(|case| input(&case.input_parts)).collect();
    for (case, text) in cases.iter().zip(&texts) {
        let mut start = 0;
        for (bytes, vector) in case.segment_utf8_bytes.iter().zip(&case.vectors) {
            let end = start + bytes;
            let original = text.get(start..end).unwrap().to_owned();
            if let Some(previous) = declared.insert(original, vector.clone()) {
                assert_eq!(previous, *vector);
            }
            start = end;
        }
        assert_eq!(start, text.len());
    }
    let server = Server::start(Arc::new(move |_, inputs| {
        let (status, mut response) = success(
            inputs
                .iter()
                .map(|text| {
                    declared
                        .get(text)
                        .expect("unknown original segment")
                        .clone()
                })
                .collect(),
        );
        response["data"].as_array_mut().unwrap().reverse();
        (status, response)
    }))
    .await;
    let result = tokio::time::timeout(DEADLINE, provider(&server, "text-full-v2", 2).embed(&texts))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.len(), cases.len());
    for (actual, expected) in result.iter().zip(&cases) {
        close(actual, &expected.expected_unit_vector);
    }
    let calls = server.finish().await;
    assert!(calls
        .iter()
        .all(|batch| !batch.is_empty() && batch.len() <= 2));
    assert_eq!(
        calls.into_iter().flatten().collect::<String>(),
        texts.concat()
    );
}

#[tokio::test]
async fn sdk_original_two_million_bytes_and_final_suffix_contribute_in_ten_bounded_calls() {
    let large = fixtures().original_full_workload_capacity;
    let original = input(&large.input_parts);
    let expected_hash = large.input_sha256.clone();
    let server = Server::start(Arc::new(move |ordinal, inputs| {
        assert!(ordinal < 10);
        assert_eq!(inputs.len(), if ordinal == 9 { 77 } else { 100 });
        success(inputs.iter().enumerate().map(|(index, text)| {
            if ordinal == 9 && index == 76 {
                assert_eq!(text.len(), 1152);
                assert!(text.ends_with("\nHelios final original evidence: Mira keeps the violet key; capacity is 47.\n"));
                vec![0.0, 1.0]
            } else {
                assert_eq!(text.len(), 2048);
                vec![1.0, 0.0]
            }
        }).collect())
    })).await;
    let result = tokio::time::timeout(
        DEADLINE,
        provider(&server, "text-full-v2", 100).embed(std::slice::from_ref(&original)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.len(), 1);
    close(&result[0], &large.sdk_expected_unit_vector);
    assert_ne!(
        result[0],
        vec![1.0, 0.0],
        "original final suffix was omitted"
    );
    let calls = server.finish().await;
    assert_eq!(calls.len(), 10);
    let physical = calls.into_iter().flatten().collect::<String>();
    assert_eq!(physical.len(), 2_000_000);
    assert_eq!(digest(physical.as_bytes()), expected_hash);
    assert_eq!(physical, original);
}

#[tokio::test]
async fn sdk_full_input_indexed_rejection_preserves_both_unicode_halves() {
    let first = "a".repeat(2047);
    let full = first.clone() + "🦊Z";
    let expected_first = first.clone();
    let server = Server::start(Arc::new(move |ordinal, inputs| match ordinal {
        0 => {
            assert_eq!(inputs, &[expected_first.clone(), "🦊Z".into()]);
            size_error(1)
        }
        1 => {
            assert_eq!(inputs, &[expected_first.clone(), "🦊".into(), "Z".into()]);
            success(vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![0.0, 1.0]])
        }
        _ => panic!("unplanned retry"),
    }))
    .await;
    let result = tokio::time::timeout(
        DEADLINE,
        provider(&server, "text-full-v2", 3).embed(&[full]),
    )
    .await
    .unwrap()
    .unwrap();
    close(
        &result[0],
        &fixtures()
            .aggregation_cases
            .last()
            .unwrap()
            .expected_unit_vector,
    );
    assert_eq!(server.finish().await.len(), 2);
}

#[tokio::test]
async fn sdk_whole_request_rejection_preserves_complete_inputs_in_both_versions() {
    for preprocessing in ["text-v1", "text-full-v2"] {
        let texts = vec![
            "first original 🦊".into(),
            "second original é suffix".into(),
        ];
        let expected = texts.clone();
        let server = Server::start(Arc::new(move |ordinal, inputs| match ordinal {
            0 => {
                assert_eq!(inputs, &expected);
                (
                    400,
                    json!({"error":{"message":"maximum tokens per request"}}),
                )
            }
            1 => {
                assert_eq!(inputs, &expected[..1]);
                success(vec![vec![1.0, 0.0]])
            }
            2 => {
                assert_eq!(inputs, &expected[1..]);
                success(vec![vec![0.0, 1.0]])
            }
            _ => panic!("whole request split introduced an extra dispatch"),
        }))
        .await;
        let result =
            tokio::time::timeout(DEADLINE, provider(&server, preprocessing, 2).embed(&texts))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(result, vec![vec![1.0, 0.0], vec![0.0, 1.0]]);
        assert_eq!(server.finish().await.len(), 3);
    }
}

#[tokio::test]
async fn sdk_refuses_unsplittable_scalar_and_foreign_error_index_without_byte_loss() {
    for rejected_index in [0, 1] {
        let server = Server::start(Arc::new(move |ordinal, inputs| {
            assert_eq!(ordinal, 0);
            assert_eq!(inputs, &["🦊".to_owned()]);
            size_error(rejected_index)
        }))
        .await;
        let result = tokio::time::timeout(
            DEADLINE,
            provider(&server, "text-full-v2", 1).embed(&["🦊".into()]),
        )
        .await
        .unwrap();
        assert!(result.is_err());
        assert_eq!(server.finish().await.len(), 1);
    }
}

#[tokio::test]
async fn sdk_full_input_rejects_malformed_shapes_and_partial_success() {
    for mode in [
        "duplicate",
        "missing",
        "foreign_index",
        "dimension",
        "zero",
        "later_failure",
    ] {
        let server = Server::start(Arc::new(move |ordinal, inputs| {
            if mode == "later_failure" && ordinal == 1 {
                return (503, json!({"error":{"message":"controlled later failure"}}));
            }
            let (_, mut value) = success(vec![
                if mode == "zero" {
                    vec![0.0, 0.0]
                } else {
                    vec![1.0, 0.0]
                };
                inputs.len()
            ]);
            match mode {
                "duplicate" => value["data"][1]["index"] = json!(0),
                "foreign_index" => value["data"][1]["index"] = json!(2),
                "missing" => {
                    value["data"].as_array_mut().unwrap().pop();
                }
                "dimension" => value["data"][0]["embedding"] = json!([1.0]),
                _ => {}
            }
            (200, value)
        }))
        .await;
        let batch = if mode == "later_failure" { 1 } else { 2 };
        let result = tokio::time::timeout(
            DEADLINE,
            provider(&server, "text-full-v2", batch).embed(&["a".repeat(2048) + "Z"]),
        )
        .await
        .unwrap();
        assert!(result.is_err(), "{mode} returned an aggregate");
        assert_eq!(
            server.finish().await.len(),
            if mode == "later_failure" { 2 } else { 1 }
        );
    }
}

#[tokio::test]
async fn sdk_text_v1_sends_the_entire_original_and_refuses_destructive_prefix_retry() {
    let original = "a".repeat(24000) + "FINAL_SUFFIX";
    let expected = original.clone();
    let server = Server::start(Arc::new(move |ordinal, inputs| {
        assert_eq!(ordinal, 0, "v1 must not retry a shortened prefix");
        assert_eq!(inputs, std::slice::from_ref(&expected));
        size_error(0)
    }))
    .await;
    let result = tokio::time::timeout(DEADLINE, provider(&server, "text-v1", 2).embed(&[original]))
        .await
        .unwrap();
    let error = result.unwrap_err();
    assert!(matches!(
        error,
        EmbeddingError::InputTooLarge {
            preprocessing: "text-v1"
        }
    ));
    assert!(error
        .to_string()
        .contains("new text-full-v2 embedding profile"));
    assert_eq!(server.finish().await.len(), 1);
}

#[tokio::test]
async fn sdk_full_input_handles_long_document_and_unbroken_run_without_losing_tail() {
    // Synthetic reproduction of HomeCore's reported size/shape, no household
    // text. The loopback limit models provider refusal, not a tokenizer estimate.
    let original = "a ".repeat(10_848) + &"x".repeat(950) + "TAIL!";
    assert_eq!(original.chars().count(), 22_651);
    let server = Server::start(Arc::new(|_, inputs| {
        if let Some(index) = inputs.iter().position(|input| input.chars().count() > 8192) {
            return size_error(index);
        }
        success(
            inputs
                .iter()
                .map(|input| {
                    if input.ends_with("TAIL!") {
                        vec![0.0, 1.0]
                    } else {
                        vec![1.0, 0.0]
                    }
                })
                .collect(),
        )
    }))
    .await;
    let rejected = provider(&server, "text-v1", 100)
        .embed(std::slice::from_ref(&original))
        .await
        .unwrap_err();
    assert!(matches!(
        rejected,
        EmbeddingError::InputTooLarge {
            preprocessing: "text-v1"
        }
    ));
    let vectors = tokio::time::timeout(
        DEADLINE,
        provider(&server, "text-full-v2", 100).embed(std::slice::from_ref(&original)),
    )
    .await
    .unwrap()
    .unwrap();
    let calls = server.finish().await;
    assert_eq!(
        calls.len(),
        2,
        "one exact refusal followed by one explicitly selected v2 request"
    );
    assert_eq!(calls[0], vec![original.clone()]);
    assert_eq!(calls[1].concat(), original);
    assert_eq!(calls[1].len(), 12);
    assert_eq!(calls[1].last().unwrap().len(), 123);
    // Independent complete byte-weighted pooling: the final 123 bytes must
    // contribute to the returned vector, despite their position after the limit.
    let norm = 22_528_f64.hypot(123_f64);
    close(&vectors[0], &[22_528.0 / norm, 123.0 / norm]);
}

#[derive(Debug)]
struct Approval {
    binding: ResolvedModelBinding,
    revoked: Arc<AtomicBool>,
    resolutions: AtomicUsize,
}
#[async_trait]
impl EmbeddingBindingResolver for Approval {
    async fn resolve(
        &self,
        space: &str,
        operation: ModelOperation,
        profile: &str,
    ) -> EmbeddingResult<ResolvedModelBinding> {
        self.resolutions.fetch_add(1, Ordering::SeqCst);
        assert_eq!(space, "independent");
        assert_eq!(operation, ModelOperation::DocumentEmbedding);
        assert_eq!(profile, "full-v2");
        if self.revoked.load(Ordering::SeqCst) {
            return Err(EmbeddingError::ProviderError(
                "original approval revoked".into(),
            ));
        }
        Ok(self.binding.clone())
    }
}
#[derive(Debug)]
struct Secret;
impl CredentialResolver for Secret {
    fn resolve(&self, reference: &str) -> EmbeddingResult<String> {
        assert_eq!(reference, "INDEPENDENT_SYNTHETIC_KEY");
        Ok("independent-synthetic-key".into())
    }
}
#[derive(Debug)]
struct Observer {
    limit: usize,
    attempts: AtomicUsize,
    events: Mutex<Vec<(String, &'static str, ModelBindingProvenance)>>,
}
#[async_trait]
impl ProviderDispatchObserver for Observer {
    async fn before_dispatch(
        &self,
        id: &str,
        provenance: &ModelBindingProvenance,
    ) -> EmbeddingResult<()> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) >= self.limit {
            return Err(EmbeddingError::ProviderError(
                "original physical admission budget exhausted".into(),
            ));
        }
        self.events
            .lock()
            .unwrap()
            .push((id.into(), "start", provenance.clone()));
        Ok(())
    }
    async fn observe_usage(
        &self,
        id: &str,
        provenance: &ModelBindingProvenance,
        input: Option<u64>,
        output: Option<u64>,
    ) -> EmbeddingResult<()> {
        assert_eq!((input, output), (Some(7), Some(0)));
        self.events
            .lock()
            .unwrap()
            .push((id.into(), "usage", provenance.clone()));
        Ok(())
    }
    async fn dispatch_finished(
        &self,
        id: &str,
        provenance: &ModelBindingProvenance,
    ) -> EmbeddingResult<()> {
        self.events
            .lock()
            .unwrap()
            .push((id.into(), "complete", provenance.clone()));
        Ok(())
    }
}
fn approval(endpoint: String, revoked: Arc<AtomicBool>) -> Approval {
    Approval {
        binding: ResolvedModelBinding {
            provenance: ModelBindingProvenance {
                config: SpaceConfigToken {
                    space_id: "independent".into(),
                    config_id: "original-config".into(),
                    generation: 1,
                    policy_id: "original-policy".into(),
                },
                operation: ModelOperation::DocumentEmbedding,
                binding_id: "original-binding".into(),
                profile_id: "full-v2".into(),
            },
            connection: ProviderConnection {
                id: "original-connection".into(),
                provider: "openai".into(),
                account_id: "original-account".into(),
                endpoint,
                credential_ref: Some("INDEPENDENT_SYNTHETIC_KEY".into()),
                credential_generation: 1,
                revoked: false,
            },
            profile: ModelProfile {
                id: "full-v2".into(),
                connection_id: "original-connection".into(),
                model: "independent-full-input".into(),
                model_version: "fixed-control-v1".into(),
                dimensions: Some(2),
                preprocessing: "text-full-v2".into(),
            },
        },
        revoked,
        resolutions: AtomicUsize::new(0),
    }
}

#[tokio::test]
async fn sdk_each_physical_segment_keeps_original_observer_authority_and_budget() {
    for mode in ["complete", "revoke", "budget", "failure"] {
        let revoked = Arc::new(AtomicBool::new(false));
        let change = revoked.clone();
        let server = Server::start(Arc::new(move |ordinal, inputs| {
            assert_eq!(inputs.len(), 1);
            if mode == "revoke" {
                change.store(true, Ordering::SeqCst);
            }
            if mode == "failure" && ordinal == 1 {
                return (503, json!({"error":{"message":"controlled unknown usage"}}));
            }
            success(vec![vec![1.0, 0.0]])
        }))
        .await;
        let authority = Arc::new(approval(server.endpoint.clone(), revoked));
        let observer = Arc::new(Observer {
            limit: if mode == "budget" { 1 } else { 8 },
            attempts: AtomicUsize::new(0),
            events: Mutex::new(Vec::new()),
        });
        let dispatch = EmbeddingDispatch::new(
            authority.clone(),
            Arc::new(Secret),
            authority.binding.clone(),
        )
        .with_observer(Some(observer.clone()));
        let provider = provider(&server, "text-full-v2", 1)
            .with_dispatch(dispatch)
            .unwrap();
        let result = tokio::time::timeout(DEADLINE, provider.embed(&["a".repeat(2048) + "Z"]))
            .await
            .unwrap();
        let calls = server.finish().await;
        assert_eq!(result.is_ok(), mode == "complete");
        assert_eq!(
            calls.len(),
            if matches!(mode, "complete" | "failure") {
                2
            } else {
                1
            }
        );
        let events = observer.events.lock().unwrap();
        let kinds: Vec<_> = events.iter().map(|(_, kind, _)| *kind).collect();
        assert_eq!(
            kinds,
            match mode {
                "complete" => vec!["start", "usage", "complete", "start", "usage", "complete"],
                "failure" => vec!["start", "usage", "complete", "start"],
                _ => vec!["start", "usage", "complete"],
            }
        );
        for (_, _, provenance) in &*events {
            assert_eq!(*provenance, authority.binding.provenance);
        }
        assert!(events[..3].iter().all(|(id, _, _)| id == &events[0].0));
        if events.len() > 3 {
            assert_ne!(events[0].0, events[3].0);
        }
        drop(events);
        assert!(
            authority.resolutions.load(Ordering::SeqCst) >= 3,
            "later physical attempt skipped fresh authority"
        );
        assert_eq!(
            observer.attempts.load(Ordering::SeqCst),
            if mode == "revoke" { 1 } else { 2 }
        );
    }
}

#[tokio::test]
async fn sdk_size_retry_reenters_original_admission_and_never_invents_rejected_usage() {
    for mode in ["complete", "revoke", "budget"] {
        let revoked = Arc::new(AtomicBool::new(false));
        let change = revoked.clone();
        let server = Server::start(Arc::new(move |ordinal, inputs| match ordinal {
            0 => {
                assert_eq!(inputs, &["a".repeat(2047), "🦊Z".into()]);
                if mode == "revoke" {
                    change.store(true, Ordering::SeqCst);
                }
                size_error(1)
            }
            1 => {
                assert_eq!(mode, "complete", "refused retry reached physical transport");
                assert_eq!(inputs, &["a".repeat(2047), "🦊".into(), "Z".into()]);
                success(vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![0.0, 1.0]])
            }
            _ => panic!("unplanned size retry"),
        }))
        .await;
        let authority = Arc::new(approval(server.endpoint.clone(), revoked));
        let observer = Arc::new(Observer {
            limit: if mode == "budget" { 1 } else { 8 },
            attempts: AtomicUsize::new(0),
            events: Mutex::new(Vec::new()),
        });
        let dispatch = EmbeddingDispatch::new(
            authority.clone(),
            Arc::new(Secret),
            authority.binding.clone(),
        )
        .with_observer(Some(observer.clone()));
        let provider = provider(&server, "text-full-v2", 3)
            .with_dispatch(dispatch)
            .unwrap();
        let result = tokio::time::timeout(DEADLINE, provider.embed(&["a".repeat(2047) + "🦊Z"]))
            .await
            .unwrap();
        assert_eq!(result.is_ok(), mode == "complete");
        if let Ok(vectors) = result {
            assert_eq!(vectors.len(), 1);
            close(
                &vectors[0],
                &fixtures()
                    .aggregation_cases
                    .last()
                    .unwrap()
                    .expected_unit_vector,
            );
        }
        assert_eq!(
            server.finish().await.len(),
            if mode == "complete" { 2 } else { 1 }
        );
        let events = observer.events.lock().unwrap();
        let kinds: Vec<_> = events.iter().map(|(_, kind, _)| *kind).collect();
        assert_eq!(
            kinds,
            if mode == "complete" {
                vec!["start", "start", "usage", "complete"]
            } else {
                vec!["start"]
            }
        );
        for (_, _, provenance) in &*events {
            assert_eq!(*provenance, authority.binding.provenance);
        }
        if events.len() > 1 {
            assert_ne!(events[0].0, events[1].0);
            assert!(events[1..].iter().all(|(id, _, _)| id == &events[1].0));
        }
        drop(events);
        assert_eq!(
            observer.attempts.load(Ordering::SeqCst),
            if mode == "revoke" { 1 } else { 2 }
        );
        assert_eq!(
            authority.resolutions.load(Ordering::SeqCst),
            if mode == "complete" { 4 } else { 3 }
        );
    }
}

#[tokio::test]
async fn sdk_unknown_preprocessing_dispatches_nothing() {
    let server = Server::start(Arc::new(|_, _| {
        panic!("unapproved preprocessing sent input")
    }))
    .await;
    let result = tokio::time::timeout(
        DEADLINE,
        provider(&server, "text-full-v3", 1).embed(&["original".into()]),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    assert!(server.finish().await.is_empty());
}

#[tokio::test]
async fn sdk_full_input_checks_all_empty_inputs_before_any_physical_send() {
    for texts in [vec![String::new()], vec!["original".into(), String::new()]] {
        let server = Server::start(Arc::new(|_, _| {
            panic!("zero-byte individual input must refuse before physical admission")
        }))
        .await;
        let result =
            tokio::time::timeout(DEADLINE, provider(&server, "text-full-v2", 1).embed(&texts))
                .await
                .unwrap();
        assert!(result.is_err());
        assert!(server.finish().await.is_empty());
    }
    let server = Server::start(Arc::new(|_, _| panic!("empty population sent input"))).await;
    let result = tokio::time::timeout(DEADLINE, provider(&server, "text-full-v2", 1).embed(&[]))
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_empty());
    assert!(server.finish().await.is_empty());
}
