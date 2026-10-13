//! Actual tokenizer regressions; no ONNX model, download or inference fixture.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::full_input::{local_segments, Preprocessing};
use tokenizers::{
    models::wordlevel::WordLevel, pre_tokenizers::whitespace::WhitespaceSplit,
    processors::template::TemplateProcessing, Tokenizer, TruncationParams,
};

fn tokenizer(limit: usize) -> Tokenizer {
    let vocab = [
        "[UNK]", "[CLS]", "[SEP]", "a", "b", "c", "d", "tail", "é", "🦦",
    ]
    .into_iter()
    .zip(0_u32..)
    .map(|(token, index)| (token.to_string(), index))
    .collect();
    let model = WordLevel::builder()
        .vocab(vocab)
        .unk_token("[UNK]".into())
        .build()
        .unwrap();
    let mut tokenizer = Tokenizer::new(model);
    tokenizer.with_pre_tokenizer(Some(WhitespaceSplit));
    tokenizer.with_post_processor(Some(
        TemplateProcessing::builder()
            .try_single("[CLS] $A [SEP]")
            .unwrap()
            .special_tokens(vec![("[CLS]", 1), ("[SEP]", 2)])
            .build()
            .unwrap(),
    ));
    tokenizer
        .with_truncation(Some(TruncationParams {
            max_length: limit,
            ..Default::default()
        }))
        .unwrap();
    tokenizer
}

#[test]
fn tokenizer_round_trip_preserves_models_processors_and_special_token_limits() {
    let mut original = tokenizer(4);
    let mut restored =
        Tokenizer::from_bytes(original.to_string(false).unwrap().as_bytes()).unwrap();
    // Exercise the real library's generated model/processor serialization, then
    // compare token IDs as well as counts before and after removing truncation.
    for input in ["a b c d tail", "é 🦦 tail", "unknown", ""] {
        for add_special_tokens in [false, true] {
            assert_eq!(
                original
                    .encode(input, add_special_tokens)
                    .unwrap()
                    .get_ids(),
                restored
                    .encode(input, add_special_tokens)
                    .unwrap()
                    .get_ids()
            );
        }
    }
    assert_eq!(restored.get_truncation().unwrap().max_length, 4);
    assert_eq!(restored.encode("a b c d tail", true).unwrap().len(), 4);
    original.with_truncation(None).unwrap();
    restored.with_truncation(None).unwrap();
    assert_eq!(
        original.encode("a b c d tail", true).unwrap().get_ids(),
        restored.encode("a b c d tail", true).unwrap().get_ids()
    );
    assert_eq!(restored.encode("a b c d tail", true).unwrap().len(), 7);
}

#[test]
fn v1_counts_actual_special_tokens_and_refuses_original_overflow() {
    let mut tokenizer = tokenizer(6);
    let original = "a b c d tail";
    // Establish the actual library regression: its default inference tokenizer
    // would silently remove the tail while still returning a valid encoding.
    assert_eq!(tokenizer.encode(original, true).unwrap().len(), 6);
    let limit = tokenizer.get_truncation().unwrap().max_length;
    tokenizer.with_truncation(None).unwrap();
    assert_eq!(tokenizer.encode(original, true).unwrap().len(), 7);
    assert_eq!(tokenizer.encode(original, false).unwrap().len(), 5);
    let accepted = local_segments(&["a b c d".into()], Preprocessing::Exact, limit, |text| {
        Ok(tokenizer.encode(text, true).unwrap().len())
    })
    .unwrap();
    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted[0].0.text, "a b c d");
    assert_eq!(accepted[0].1, 6);
    assert!(
        local_segments(&[original.into()], Preprocessing::Exact, limit, |text| Ok(
            tokenizer.encode(text, true).unwrap().len()
        ))
        .is_err()
    );
}

#[test]
fn v2_preserves_both_halves_utf8_and_separate_inputs_under_actual_token_limit() {
    let mut tokenizer = tokenizer(4);
    let limit = tokenizer.get_truncation().unwrap().max_length;
    tokenizer.with_truncation(None).unwrap();
    let inputs = vec!["a b c d tail".to_string(), "é 🦦 é 🦦 tail".to_string()];
    let accepted = local_segments(&inputs, Preprocessing::Full, limit, |text| {
        Ok(tokenizer.encode(text, true).unwrap().len())
    })
    .unwrap();
    assert!(accepted.len() > inputs.len());
    for (input, original) in inputs.iter().enumerate() {
        let parts: Vec<_> = accepted
            .iter()
            .filter(|(part, _)| part.input == input)
            .collect();
        let reconstructed: String = parts.iter().map(|(part, _)| part.text.as_str()).collect();
        assert_eq!(&reconstructed, original);
        assert!(reconstructed.ends_with("tail"));
        let mut end = 0;
        for (part, tokens) in parts {
            assert_eq!(part.start, end);
            assert_eq!(part.text, original[part.start..part.end]);
            assert!(original.is_char_boundary(part.start));
            assert!(original.is_char_boundary(part.end));
            assert_eq!(
                *tokens,
                u64::try_from(tokenizer.encode(&part.text[..], true).unwrap().len()).unwrap()
            );
            assert!(*tokens <= 4);
            end = part.end;
        }
        assert_eq!(end, original.len());
    }
}

#[test]
fn only_v1_empty_input_keeps_special_token_usage_and_unsplittable_v2_refuses() {
    let mut tokenizer = tokenizer(3);
    tokenizer.with_truncation(None).unwrap();
    let accepted = local_segments(&[String::new()], Preprocessing::Exact, 2, |text| {
        Ok(tokenizer.encode(text, true).unwrap().len())
    })
    .unwrap();
    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted[0].0.start, 0);
    assert_eq!(accepted[0].0.end, 0);
    assert_eq!(accepted[0].1, 2);
    assert!(
        local_segments(&[String::new()], Preprocessing::Full, 3, |_| unreachable!(
            "empty v2 must refuse before tokenizer or inference"
        ))
        .is_err()
    );
    assert!(
        local_segments(&["🦦".into()], Preprocessing::Full, 2, |text| Ok(
            tokenizer.encode(text, true).unwrap().len()
        ))
        .is_err()
    );
    assert!(
        local_segments(&[String::new()], Preprocessing::Full, 1, |text| Ok(
            tokenizer.encode(text, true).unwrap().len()
        ))
        .is_err()
    );
}

#[test]
fn tokenizer_failure_after_first_segment_never_returns_a_partial_plan() {
    let inputs = vec!["a ".repeat(2048)];
    let mut calls = 0;
    let result = local_segments(&inputs, Preprocessing::Full, 2048, |_| {
        calls += 1;
        if calls == 1 {
            Ok(1)
        } else {
            Err(super::full_input::invalid("tokenizer failed"))
        }
    });
    assert!(result.is_err());
    assert_eq!(calls, 2);
}

#[derive(Debug)]
struct LocalApproval {
    binding: elephant_types::provider_binding::ResolvedModelBinding,
    revoked: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl crate::governed::EmbeddingBindingResolver for LocalApproval {
    async fn resolve(
        &self,
        _: &str,
        _: elephant_types::provider_binding::ModelOperation,
        _: &str,
    ) -> super::EmbeddingResult<elephant_types::provider_binding::ResolvedModelBinding> {
        if self.revoked.load(std::sync::atomic::Ordering::Acquire) {
            Err(super::full_input::invalid(
                "revoked while blocking task queued",
            ))
        } else {
            Ok(self.binding.clone())
        }
    }
}

#[derive(Debug)]
struct NoLocalCredential;
impl crate::governed::CredentialResolver for NoLocalCredential {
    fn resolve(&self, _: &str) -> super::EmbeddingResult<String> {
        Err(super::full_input::invalid(
            "local inference needs no credential",
        ))
    }
}

fn local_approval() -> std::sync::Arc<LocalApproval> {
    use elephant_types::provider_binding::*;
    std::sync::Arc::new(LocalApproval {
        binding: ResolvedModelBinding {
            provenance: ModelBindingProvenance {
                config: SpaceConfigToken {
                    space_id: "local-input-test".into(),
                    config_id: "local-config".into(),
                    generation: 1,
                    policy_id: "local-policy".into(),
                },
                operation: ModelOperation::QueryEmbedding,
                binding_id: "local-binding".into(),
                profile_id: "local-profile".into(),
            },
            connection: ProviderConnection {
                id: "local-connection".into(),
                provider: "fastembed".into(),
                account_id: "local-test".into(),
                endpoint: "file:///local-input-test-assets".into(),
                credential_ref: None,
                credential_generation: 1,
                revoked: false,
            },
            profile: ModelProfile {
                id: "local-profile".into(),
                connection_id: "local-connection".into(),
                model: "bge-small-en-v1.5".into(),
                model_version: format!("sha256:{}", "a".repeat(64)),
                dimensions: Some(384),
                preprocessing: "text-full-v2".into(),
            },
        },
        revoked: std::sync::atomic::AtomicBool::new(false),
    })
}

#[test]
fn bge_retrieval_query_instruction_preserves_every_original_byte() {
    use elephant_types::provider_binding::{ModelOperation, BGE_EN_RETRIEVAL_QUERY_INSTRUCTION};
    let originals = vec![
        "How do I shut off the outdoor faucet?".into(),
        "unrelated corpus: é 🦦\n\t tail  ".into(),
        format!("{BGE_EN_RETRIEVAL_QUERY_INSTRUCTION}already part of the source query"),
        String::new(),
    ];
    let actual = super::full_input::local_inputs(
        &originals,
        Preprocessing::BgeEnRetrieval,
        Some(ModelOperation::QueryEmbedding),
    )
    .unwrap();
    for (before, after) in originals.iter().zip(actual) {
        assert_eq!(
            after,
            format!("Represent this sentence for searching relevant passages: {before}")
        );
        assert_eq!(
            &after.as_bytes()[BGE_EN_RETRIEVAL_QUERY_INSTRUCTION.len()..],
            before.as_bytes()
        );
    }
    for role in [
        ModelOperation::DocumentEmbedding,
        ModelOperation::ReindexEmbedding,
    ] {
        assert_eq!(
            super::full_input::local_inputs(&originals, Preprocessing::BgeEnRetrieval, Some(role))
                .unwrap(),
            originals
        );
    }
    for legacy in [Preprocessing::Exact, Preprocessing::Full] {
        assert_eq!(
            super::full_input::local_inputs(
                &originals,
                legacy,
                Some(ModelOperation::QueryEmbedding)
            )
            .unwrap(),
            originals
        );
    }
}

#[test]
fn bge_retrieval_requires_request_role_and_counts_instruction_at_actual_token_boundary() {
    use elephant_types::provider_binding::ModelOperation;
    let original = vec!["é 🦦 tail".into()];
    for role in [
        None,
        Some(ModelOperation::Extraction),
        Some(ModelOperation::SupportVerification),
    ] {
        assert!(
            super::full_input::local_inputs(&original, Preprocessing::BgeEnRetrieval, role)
                .is_err()
        );
    }
    let inputs = super::full_input::local_inputs(
        &original,
        Preprocessing::BgeEnRetrieval,
        Some(ModelOperation::QueryEmbedding),
    )
    .unwrap();
    let mut tokenizer = tokenizer(512);
    tokenizer.with_truncation(None).unwrap();
    let count = tokenizer.encode(inputs[0].as_str(), true).unwrap().len();
    assert!(count > tokenizer.encode(original[0].as_str(), true).unwrap().len());
    let accepted = local_segments(&inputs, Preprocessing::BgeEnRetrieval, count, |text| {
        Ok(tokenizer.encode(text, true).unwrap().len())
    })
    .unwrap();
    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted[0].0.text, inputs[0]);
    assert_eq!(accepted[0].1, count as u64);
    assert!(local_segments(
        &inputs,
        Preprocessing::BgeEnRetrieval,
        count - 1,
        |text| Ok(tokenizer.encode(text, true).unwrap().len())
    )
    .is_err());
}

#[test]
fn bge_retrieval_readiness_is_provider_model_and_operation_exact() {
    use elephant_types::provider_binding::{ModelOperation, BGE_EN_RETRIEVAL_V1};
    let mut binding = local_approval().binding.clone();
    binding.profile.preprocessing = BGE_EN_RETRIEVAL_V1.into();
    for operation in [
        ModelOperation::QueryEmbedding,
        ModelOperation::DocumentEmbedding,
        ModelOperation::ReindexEmbedding,
    ] {
        binding.provenance.operation = operation;
        let recipe = super::embedding_execution_recipe(&binding).unwrap();
        assert_eq!(recipe.provenance.operation, operation);
        assert_eq!(recipe.partition.preprocessing, BGE_EN_RETRIEVAL_V1);
        assert_eq!(recipe.max_total_http_attempts, None);
        assert_eq!(recipe.initial_segment_bytes, None);
        assert_eq!(recipe.max_split_depth, None);
        assert!(super::approved_openai_config(&binding, "unused".into()).is_err());
    }
    binding.connection.provider = "openai".into();
    assert!(super::embedding_execution_recipe(&binding).is_err());
    binding.connection.provider = "fastembed".into();
    binding.profile.model = "all-minilm-l6-v2".into();
    assert!(super::embedding_execution_recipe(&binding).is_err());
}

type BgeUsageEvent = (
    String,
    elephant_types::provider_binding::ModelOperation,
    &'static str,
    Option<u64>,
);

#[derive(Debug, Default)]
struct BgeUsage(std::sync::Mutex<Vec<BgeUsageEvent>>);

#[tokio::test]
async fn bge_retrieval_changed_operation_cannot_dispatch_prepared_query() {
    use elephant_types::provider_binding::{ModelOperation, BGE_EN_RETRIEVAL_V1};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    let mut approval = Arc::try_unwrap(local_approval()).unwrap();
    approval.binding.profile.preprocessing = BGE_EN_RETRIEVAL_V1.into();
    let pinned = approval.binding.clone();
    approval.binding.provenance.operation = ModelOperation::DocumentEmbedding;
    let observer = Arc::new(BgeUsage::default());
    let dispatch = crate::governed::EmbeddingDispatch::new(
        Arc::new(approval),
        Arc::new(NoLocalCredential),
        pinned,
    )
    .with_observer(Some(observer.clone()));
    let prepared = super::full_input::local_inputs(
        &["query".into()],
        Preprocessing::BgeEnRetrieval,
        Some(dispatch.operation()),
    )
    .unwrap();
    assert!(prepared[0].starts_with("Represent this sentence"));
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    assert!(super::local_dispatch::run(
        Some(dispatch),
        "bge-small-en-v1.5".into(),
        384,
        Preprocessing::BgeEnRetrieval,
        10,
        move || {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    )
    .await
    .is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(observer.0.lock().unwrap().is_empty());
}

#[async_trait::async_trait]
impl crate::governed::ProviderDispatchObserver for BgeUsage {
    async fn before_dispatch(
        &self,
        id: &str,
        provenance: &elephant_types::provider_binding::ModelBindingProvenance,
    ) -> super::EmbeddingResult<()> {
        self.0
            .lock()
            .unwrap()
            .push((id.into(), provenance.operation, "start", None));
        Ok(())
    }
    async fn observe_usage(
        &self,
        id: &str,
        provenance: &elephant_types::provider_binding::ModelBindingProvenance,
        input: Option<u64>,
        output: Option<u64>,
    ) -> super::EmbeddingResult<()> {
        assert_eq!(output, Some(0));
        self.0
            .lock()
            .unwrap()
            .push((id.into(), provenance.operation, "usage", input));
        Ok(())
    }
    async fn dispatch_finished(
        &self,
        id: &str,
        provenance: &elephant_types::provider_binding::ModelBindingProvenance,
    ) -> super::EmbeddingResult<()> {
        self.0
            .lock()
            .unwrap()
            .push((id.into(), provenance.operation, "complete", None));
        Ok(())
    }
}

#[tokio::test]
async fn bge_retrieval_dispatch_accounts_for_transformed_tokens_with_separate_role_receipts() {
    use elephant_types::provider_binding::{ModelOperation, BGE_EN_RETRIEVAL_V1};
    use std::sync::Arc;
    let observer = Arc::new(BgeUsage::default());
    let mut tokenizer = tokenizer(512);
    tokenizer.with_truncation(None).unwrap();
    let mut expected_tokens = Vec::new();
    for operation in [
        ModelOperation::QueryEmbedding,
        ModelOperation::DocumentEmbedding,
        ModelOperation::ReindexEmbedding,
    ] {
        let mut approval = Arc::try_unwrap(local_approval()).unwrap();
        approval.binding.provenance.operation = operation;
        approval.binding.profile.preprocessing = BGE_EN_RETRIEVAL_V1.into();
        let approval = Arc::new(approval);
        let dispatch = crate::governed::EmbeddingDispatch::new(
            approval.clone(),
            Arc::new(NoLocalCredential),
            approval.binding.clone(),
        )
        .with_observer(Some(observer.clone()));
        let inputs = super::full_input::local_inputs(
            &["é 🦦 tail".into()],
            Preprocessing::BgeEnRetrieval,
            Some(dispatch.operation()),
        )
        .unwrap();
        let batch = local_segments(&inputs, Preprocessing::BgeEnRetrieval, 512, |text| {
            Ok(tokenizer.encode(text, true).unwrap().len())
        })
        .unwrap();
        let tokens = batch.iter().map(|(_, tokens)| *tokens).sum();
        expected_tokens.push(tokens);
        let result = super::local_dispatch::run(
            Some(dispatch),
            approval.binding.profile.model.clone(),
            384,
            Preprocessing::BgeEnRetrieval,
            tokens,
            move || {
                Ok(batch
                    .into_iter()
                    .map(|(segment, _)| segment.text)
                    .collect::<Vec<_>>())
            },
        )
        .await
        .unwrap();
        assert_eq!(result, inputs);
    }
    assert!(expected_tokens[0] > expected_tokens[1]);
    assert_eq!(expected_tokens[1], expected_tokens[2]);
    let events = observer.0.lock().unwrap();
    assert_eq!(events.len(), 9);
    for (group, tokens) in events.chunks_exact(3).zip(expected_tokens) {
        assert_eq!(
            group.iter().map(|event| event.2).collect::<Vec<_>>(),
            ["start", "usage", "complete"]
        );
        assert!(group
            .iter()
            .all(|event| event.0 == group[0].0 && event.1 == group[0].1));
        assert_eq!(group[1].3, Some(tokens));
    }
    assert_ne!(events[0].0, events[3].0);
    assert_ne!(events[3].0, events[6].0);
    drop(events);
}

#[test]
fn local_blocking_queue_rechecks_revocation_before_actual_inference() {
    check_local_queue_revocation("text-full-v2", Preprocessing::Full);
}

#[test]
fn bge_retrieval_blocking_queue_rechecks_revocation_before_actual_inference() {
    check_local_queue_revocation(
        elephant_types::provider_binding::BGE_EN_RETRIEVAL_V1,
        Preprocessing::BgeEnRetrieval,
    );
}

fn check_local_queue_revocation(recipe: &str, preprocessing: Preprocessing) {
    use std::{
        future::Future,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        task::Poll,
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (release, waiting) = std::sync::mpsc::channel();
        let (started, ready) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            waiting
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
        });
        ready
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let mut approval = Arc::try_unwrap(local_approval()).unwrap();
        approval.binding.profile.preprocessing = recipe.into();
        let approval = Arc::new(approval);
        let dispatch = crate::governed::EmbeddingDispatch::new(
            approval.clone(),
            Arc::new(NoLocalCredential),
            approval.binding.clone(),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let mut inference = Box::pin(super::local_dispatch::run(
            Some(dispatch),
            approval.binding.profile.model.clone(),
            384,
            preprocessing,
            3,
            move || {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        ));
        // Poll the real adapter to its queued JoinHandle, while the sole
        // blocking thread remains occupied. No timing or scheduler guess.
        std::future::poll_fn(|cx| {
            assert!(inference.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        approval.revoked.store(true, Ordering::Release);
        release.send(()).unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), inference)
                .await
                .unwrap()
                .is_err()
        );
        blocker.await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        // Same original binding and adapter remain usable when currently
        // approved; this negative must not pass because the fixture is invalid.
        approval.revoked.store(false, Ordering::Release);
        let dispatch = crate::governed::EmbeddingDispatch::new(
            approval.clone(),
            Arc::new(NoLocalCredential),
            approval.binding.clone(),
        );
        let count = calls.clone();
        super::local_dispatch::run(
            Some(dispatch),
            approval.binding.profile.model.clone(),
            384,
            preprocessing,
            3,
            move || {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn cancelled_local_blocking_queue_never_starts_inference() {
    use std::{
        future::Future,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        task::Poll,
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (release, waiting) = std::sync::mpsc::channel();
        let (started, ready) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            waiting
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
        });
        ready
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let mut inference = Box::pin(super::local_dispatch::run(
            None,
            "local-test".into(),
            2,
            Preprocessing::Full,
            3,
            move || {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        ));
        std::future::poll_fn(|cx| {
            assert!(inference.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(inference);
        release.send(()).unwrap();
        blocker.await.unwrap();
        // Drain the same one-thread FIFO before inspecting physical callbacks.
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::task::spawn_blocking(|| ()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    });
}

#[tokio::test]
async fn warmed_resource_with_fresh_dispatch_rechecks_revocation_after_model_mutex_wait() {
    use std::{
        future::Future,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        task::Poll,
        time::Duration,
    };
    let slot = crate::local_model_cache::ResourceSlot::default();
    let loads = Arc::new(AtomicUsize::new(0));
    let first_count = loads.clone();
    let model = slot
        .get_or_try_init("exact-manifest", move |capacity| {
            first_count.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(tokio::sync::Mutex::new(capacity)))
        })
        .await
        .unwrap();
    let held = model.clone().lock_owned().await;
    drop(model);
    // A fresh request gets the same resource and its own dispatch authority.
    let next_count = loads.clone();
    let warmed = slot
        .get_or_try_init("exact-manifest", move |capacity| {
            next_count.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(tokio::sync::Mutex::new(capacity)))
        })
        .await
        .unwrap();
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    let approval = local_approval();
    let dispatch = crate::governed::EmbeddingDispatch::new(
        approval.clone(),
        Arc::new(NoLocalCredential),
        approval.binding.clone(),
    );
    dispatch.authorize().await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut request = Box::pin(async move {
        let executing = warmed.lock_owned().await;
        super::local_dispatch::run(
            Some(dispatch),
            "bge-small-en-v1.5".into(),
            384,
            Preprocessing::Full,
            3,
            move || {
                let _executing = executing;
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .await
    });
    std::future::poll_fn(|cx| {
        assert!(request.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    approval.revoked.store(true, Ordering::Release);
    drop(held);
    assert!(tokio::time::timeout(Duration::from_secs(5), request)
        .await
        .unwrap()
        .is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(loads.load(Ordering::SeqCst), 1);
}
