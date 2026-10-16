// Included inside provider_dispatch::model::tests; only loopback HTTP is used.
type SummaryPolicyTestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn summary_policy_fixture(
    endpoint: &str,
) -> SummaryPolicyTestResult<(SpaceConfigRepository, ProviderDispatchContext)> {
    let processing: SpaceProcessingConfig = serde_json::from_value(serde_json::json!({
        "generation": 1, "index_generation": 0, "policy_id": "summary-fixture",
        "connections": {"account": {"id": "account", "provider": "anthropic",
            "account_id": "fixture-account", "endpoint": endpoint,
            "credential_ref": "fixture-key", "credential_generation": 1, "revoked": false}},
        "profiles": {"summary-v1": {"id": "summary-v1", "connection_id": "account",
            "model": "claude-sonnet-5-5", "model_version": "fixture-v1",
            "dimensions": null, "preprocessing": "text-v1",
            "summary": {"max_output_tokens": 2048, "max_summary_length": 8000,
                "thinking": "between_tools_low"}}},
        "bindings": {"summary": {"id": "summary", "profile_id": "summary-v1", "revoked": false}},
        "active_embedding_profile_id": null, "pending_reindex": null
    }))?;
    processing.validate()?;
    let repository = SpaceConfigRepository::new(StorageClient::new_embedded().await?);
    repository
        .create(SpaceConfig {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SpaceConfig),
                "summary-fixture".into(),
                "test".into(),
            ),
            embedding_provider: "openai".into(),
            embedding_model: "unused".into(),
            embedding_dims: 2,
            processing: Some(processing),
            purpose: None,
            skill: None,
        })
        .await?;
    let context = ProviderDispatchContext::new(repository.clone(), "summary-fixture")
        .with_credentials(Arc::new(TestCredentials));
    Ok((repository, context))
}

fn summary_policy_context() -> crate::summarizer::SummarizationContext {
    crate::summarizer::SummarizationContext::new(
        "Original fixture text. ".repeat(70),
        crate::summarizer::SummarizationBudget::default(),
        crate::summarizer::SummarizationOptions::default(),
    )
}

async fn answer_summary_request(
    listener: &tokio::net::TcpListener,
) -> SummaryPolicyTestResult<(String, serde_json::Value)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (mut socket, _) = listener.accept().await?;
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1024];
    let end = loop {
        let read = socket.read(&mut chunk).await?;
        if read == 0 || bytes.len() + read > 65_536 {
            return Err("incomplete or oversized local request".into());
        }
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let headers = std::str::from_utf8(&bytes[..end])?.to_owned();
    let length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>())
        })
        .ok_or("missing local request length")??;
    if length > 65_536 - end {
        return Err("oversized local body".into());
    }
    while bytes.len() < end + length {
        let read = socket.read(&mut chunk).await?;
        if read == 0 || bytes.len() + read > 65_536 {
            return Err("incomplete or oversized local body".into());
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
    let body = serde_json::from_slice(&bytes[end..end + length])?;
    let events = [
        serde_json::json!({"type":"message_start","message":{"usage":{"input_tokens":10,"output_tokens":0}}}),
        serde_json::json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Complete local summary."}}),
        serde_json::json!({"type":"content_block_stop","index":0}),
        serde_json::json!({"type":"message_delta","usage":{"output_tokens":5},"delta":{"stop_reason":"end_turn"}}),
        serde_json::json!({"type":"message_stop"}),
    ];
    let response = events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect::<String>();
    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await?;
    socket.shutdown().await?;
    Ok((headers, body))
}

#[tokio::test]
async fn governed_summary_custom_policy_reaches_real_local_request() -> SummaryPolicyTestResult {
    use crate::summarizer::Summarizer;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let (_repository, context) = summary_policy_fixture(&endpoint).await?;
    let (model, _client, policy) = context.summary_client().await?;
    assert_eq!(model, "claude-sonnet-5-5");
    assert_eq!(policy.max_output_tokens, 2048);
    assert_eq!(policy.max_summary_length, 8000);
    let summarizer = crate::summarizer::AgenticSummarizer::new().with_provider_context(context);
    let input = summary_policy_context();
    assert!(input.text.chars().count() > input.options.min_text_length);
    assert!(input.text.chars().count() < policy.max_summary_length);
    let (summary, request) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(
            summarizer.summarize(&input),
            answer_summary_request(&listener)
        )
    })
    .await?;
    assert_eq!(summary?.text, "Complete local summary.");
    let (headers, body) = request?;
    assert!(headers.starts_with("POST /v1/messages HTTP/1.1"));
    assert!(headers
        .to_ascii_lowercase()
        .contains("x-api-key: local-fixture-secret"));
    assert_eq!(body["model"], model);
    assert_eq!(body["max_tokens"], 2048);
    assert_eq!(body["thinking"]["type"], "between_tools");
    assert_eq!(body["output_config"]["effort"], "low");
    assert!(body
        .get("tools")
        .is_none_or(|tools| tools.as_array().is_some_and(Vec::is_empty)));
    let messages = body["messages"].to_string();
    assert!(messages.contains("8000 characters and within 2048 tokens"));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn governed_summary_revocation_during_physical_authorization_never_falls_back(
) -> SummaryPolicyTestResult {
    use crate::summarizer::Summarizer;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let (repository, context) = summary_policy_fixture(&endpoint).await?;
    let observer = Arc::new(RevokeBeforeSend {
        repository,
        calls: AtomicUsize::new(0),
    });
    let summarizer = crate::summarizer::AgenticSummarizer::new()
        .with_provider_context(context.with_observer(observer.clone()));
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        summarizer.summarize(&summary_policy_context()),
    )
    .await?;
    let error = result
        .err()
        .ok_or("revoked summary unexpectedly returned text or fallback")?;
    assert_eq!(
        crate::scheduler::execution_contract::classify_failure(&error.into()),
        crate::scheduler::execution_contract::FailureClass::ConfigurationBlocked
    );
    assert_eq!(observer.calls.load(Ordering::SeqCst), 1);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn governed_summary_pinned_policy_refuses_replaced_profile_before_dispatch(
) -> SummaryPolicyTestResult {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let (repository, context) = summary_policy_fixture(&endpoint).await?;
    let (model, client, policy) = context.summary_client().await?;
    let original = repository
        .get_by_space_id("summary-fixture")
        .await?
        .ok_or("missing fixture")?;
    let mut successor = original.clone();
    let processing = successor.processing.as_mut().ok_or("missing processing")?;
    let mut replacement = processing
        .profiles
        .get("summary-v1")
        .ok_or("missing profile")?
        .clone();
    replacement.id = "summary-v2".into();
    replacement
        .summary
        .as_mut()
        .ok_or("missing policy")?
        .max_output_tokens = 1024;
    processing
        .profiles
        .insert(replacement.id.clone(), replacement);
    processing
        .bindings
        .get_mut(&ModelOperation::Summary)
        .ok_or("missing summary binding")?
        .profile_id = "summary-v2".into();
    processing.generation += 1;
    repository.replace_authorized(&original, successor).await?;
    let result =
        crate::llm::run_summary_prompt(client, model, "original approved request".into(), &policy)
            .await;
    let error = result
        .err()
        .ok_or("stale summary unexpectedly dispatched")?;
    assert_eq!(
        crate::scheduler::execution_contract::classify_failure(&error.into()),
        crate::scheduler::execution_contract::FailureClass::ConfigurationBlocked
    );
    let (_, _new_client, current_policy) = context.summary_client().await?;
    assert_eq!(current_policy.max_output_tokens, 1024);
    assert_eq!(policy.max_output_tokens, 2048);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    Ok(())
}
