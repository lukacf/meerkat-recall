// Included in provider_dispatch::model::tests. Only a localhost listener is used.
#[tokio::test]
#[expect(
    clippy::expect_used,
    reason = "Unexpected dispatch success must fail at the original revocation assertion after the shared admission wait."
)]
async fn queued_generation_rechecks_approval_after_shared_remote_wait() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
    let first = elephant_embeddings::remote_admission::acquire(&endpoint)
        .await
        .unwrap();
    let second = elephant_embeddings::remote_admission::acquire(&endpoint)
        .await
        .unwrap();
    let processing: SpaceProcessingConfig = serde_json::from_value(serde_json::json!({
        "generation":1, "index_generation":0, "policy_id":"fixture-policy",
        "connections":{"account":{"id":"account","provider":"openai","account_id":"fixture-account","endpoint":endpoint,"credential_ref":"fixture-key","credential_generation":1,"revoked":false}},
        "profiles":{"model":{"id":"model","connection_id":"account","model":"gpt-4.1-mini","model_version":"fixture","dimensions":null,"preprocessing":"text-v1"}},
        "bindings":{"summary":{"id":"summary","profile_id":"model","revoked":false}},
        "active_embedding_profile_id":null, "pending_reindex":null
    })).unwrap();
    let repository = SpaceConfigRepository::new(StorageClient::new_embedded().await.unwrap());
    let before = repository
        .create(SpaceConfig {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SpaceConfig),
                "queued-fixture".into(),
                "test".into(),
            ),
            embedding_provider: "openai".into(),
            embedding_model: "unused".into(),
            embedding_dims: 3,
            processing: Some(processing),
            purpose: None,
            skill: None,
        })
        .await
        .unwrap();
    let context = ProviderDispatchContext::new(repository.clone(), "queued-fixture")
        .with_credentials(Arc::new(TestCredentials));
    let (model, client, _) = context.model_client(ModelOperation::Summary).await.unwrap();
    let mut pending = Box::pin(crate::llm::run_model_prompt(
        client,
        model,
        None,
        "queued original prompt".into(),
        10,
    ));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut pending)
            .await
            .is_err()
    );
    let mut revoked = before.clone();
    let processing = revoked.processing.as_mut().unwrap();
    processing.generation += 1;
    processing
        .bindings
        .get_mut(&ModelOperation::Summary)
        .unwrap()
        .revoked = true;
    repository
        .replace_authorized(&before, revoked)
        .await
        .unwrap();
    drop(first);
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), &mut pending)
        .await
        .unwrap();
    let error = result.expect_err("revocation must prevent sending after admission wait");
    assert_eq!(
        crate::scheduler::execution_contract::classify_failure(&error.into()),
        crate::scheduler::execution_contract::FailureClass::ConfigurationBlocked
    );
    drop(pending);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), listener.accept())
            .await
            .is_err()
    );
    drop(second);
    let _first = elephant_embeddings::remote_admission::acquire(&endpoint)
        .await
        .unwrap();
    let _second = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        elephant_embeddings::remote_admission::acquire(&endpoint),
    )
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test]
async fn incomplete_generation_and_embedding_share_capacity_until_stream_cancellation() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn read_request(socket: &mut tokio::net::TcpStream, route: &str) {
        let mut bytes = Vec::new();
        loop {
            let mut chunk = [0; 4096];
            let count = socket.read(&mut chunk).await.unwrap();
            assert!(count > 0);
            bytes.extend_from_slice(&chunk[..count]);
            assert!(bytes.len() <= 64 * 1024);
            if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..end]);
                assert!(headers.starts_with(&format!("POST {route} ")));
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':')
                            .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                            .map(|(_, value)| value.trim().parse().unwrap())
                    })
                    .unwrap();
                if bytes.len() >= end + 4 + length {
                    return;
                }
            }
        }
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
    let processing: SpaceProcessingConfig = serde_json::from_value(serde_json::json!({
        "generation":1,"index_generation":0,"policy_id":"fixture-policy",
        "connections":{"account":{"id":"account","provider":"openai","account_id":"shared-fixture-account","endpoint":endpoint,"credential_ref":"fixture-key","credential_generation":1,"revoked":false}},
        "profiles":{
            "model":{"id":"model","connection_id":"account","model":"gpt-4.1-mini","model_version":"fixture","dimensions":null,"preprocessing":"text-v1"},
            "embedding":{"id":"embedding","connection_id":"account","model":"test-embed","model_version":"fixture","dimensions":3,"preprocessing":"text-v1"}},
        "bindings":{
            "summary":{"id":"summary","profile_id":"model","revoked":false},
            "query_embedding":{"id":"query","profile_id":"embedding","revoked":false},
            "document_embedding":{"id":"document","profile_id":"embedding","revoked":false}},
        "active_embedding_profile_id":"embedding","pending_reindex":null
    })).unwrap();
    let repository = SpaceConfigRepository::new(StorageClient::new_embedded().await.unwrap());
    repository
        .create(SpaceConfig {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SpaceConfig),
                "mixed-fixture".into(),
                "test".into(),
            ),
            embedding_provider: "openai".into(),
            embedding_model: "test-embed".into(),
            embedding_dims: 3,
            processing: Some(processing),
            purpose: None,
            skill: None,
        })
        .await
        .unwrap();
    let context = ProviderDispatchContext::new(repository, "mixed-fixture")
        .with_credentials(Arc::new(TestCredentials));
    let (model, client, _) = context.model_client(ModelOperation::Summary).await.unwrap();
    let (_, embedding, _) = context
        .embedding(ModelOperation::QueryEmbedding)
        .await
        .unwrap();
    let generation = tokio::spawn(crate::llm::run_model_prompt(
        client,
        model,
        None,
        "incomplete local stream".into(),
        10,
    ));
    let (mut model_socket, _) =
        tokio::time::timeout(std::time::Duration::from_secs(2), listener.accept())
            .await
            .unwrap()
            .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_request(&mut model_socket, "/v1/responses"),
    )
    .await
    .unwrap();
    model_socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 256\r\nConnection: close\r\n\r\n").await.unwrap();
    let first_embedding = embedding.clone();
    let first =
        tokio::spawn(async move { first_embedding.embed(&["held embedding".into()]).await });
    let (mut embedding_socket, _) =
        tokio::time::timeout(std::time::Duration::from_secs(2), listener.accept())
            .await
            .unwrap()
            .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_request(&mut embedding_socket, "/v1/embeddings"),
    )
    .await
    .unwrap();
    embedding_socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 256\r\nConnection: close\r\n\r\n{\"data\":[").await.unwrap();
    let third = tokio::spawn(async move { embedding.embed(&["queued embedding".into()]).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    generation.abort();
    assert!(generation.await.unwrap_err().is_cancelled());
    let (mut third_socket, _) =
        tokio::time::timeout(std::time::Duration::from_secs(2), listener.accept())
            .await
            .unwrap()
            .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_request(&mut third_socket, "/v1/embeddings"),
    )
    .await
    .unwrap();
    let body = r#"{"data":[{"index":0,"embedding":[0.5,0.5,0.5]}],"model":"test-embed","usage":{"prompt_tokens":7,"total_tokens":7}}"#;
    third_socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), third)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        vec![vec![0.5, 0.5, 0.5]]
    );
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    drop((model_socket, embedding_socket, third_socket));
    let _one = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        elephant_embeddings::remote_admission::acquire(&endpoint),
    )
    .await
    .unwrap()
    .unwrap();
    let _two = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        elephant_embeddings::remote_admission::acquire(&endpoint),
    )
    .await
    .unwrap()
    .unwrap();
}
