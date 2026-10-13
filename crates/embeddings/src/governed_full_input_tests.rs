// Actual governed factory -> configured HTTP provider. No provider credentials,
// model assets or external service are used by this loopback regression.
#[tokio::test]
#[cfg_attr(
    feature = "fastembed",
    expect(
        clippy::significant_drop_tightening,
        reason = "Keep the provider and original input chronology through transport and usage assertions."
    )
)]
async fn governed_profile_controls_full_input_without_relabeling_v1() {
    for preprocessing in ["text-v1", "text-full-v2"] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut approved = approval(format!("http://{}/v1", listener.local_addr().unwrap()));
        approved
            .space
            .processing
            .profiles
            .get_mut("p")
            .unwrap()
            .preprocessing = preprocessing.into();
        let approved = Arc::new(approved);
        let binding = approved
            .space
            .authorize("a", ModelOperation::QueryEmbedding, None)
            .unwrap();
        let observer = Arc::new(CompletionObserver::default());
        let provider = GovernedEmbeddingProvider::new(
            EmbeddingDispatch::new(approved, Arc::new(Secret), binding)
                .with_observer(Some(observer.clone())),
        )
        .unwrap();
        let original = format!("{}é🦊suffix", "a".repeat(2048));
        let expected = original.clone();
        let server = tokio::spawn(serve_governed_full_input(listener, expected, preprocessing));
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            provider.embed(&[original]),
        )
        .await
        .unwrap()
        .unwrap();
        let requests = tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.len(), 1);
        assert!(
            result[0][1] > 0.0,
            "the suffix contributes to the returned vector"
        );
        if preprocessing == "text-v1" {
            assert_eq!(result[0], vec![1.0, 1.0, 0.0]);
        } else {
            let squared_norm: f32 = result[0].iter().map(|value| value * value).sum();
            assert!((squared_norm - 1.0).abs() < 0.00001);
        }
        let events = observer.0.lock().unwrap();
        assert_eq!(events.len(), requests * 3);
        for call in events.chunks(3) {
            assert_eq!(
                call.iter().map(|(_, event)| *event).collect::<Vec<_>>(),
                ["start", "usage", "complete"]
            );
            assert!(call.iter().all(|(id, _)| id == &call[0].0));
        }
    }
}

async fn serve_governed_full_input(
    listener: tokio::net::TcpListener,
    expected: String,
    preprocessing: &'static str,
) -> usize {
    let mut received = Vec::<String>::new();
    let mut observed_bytes = 0;
    let mut requests = 0;
    while observed_bytes < expected.len() {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let request = loop {
            let mut chunk = [0; 4096];
            let count = socket.read(&mut chunk).await.unwrap();
            assert!(count > 0, "complete original request body required");
            bytes.extend_from_slice(&chunk[..count]);
            assert!(bytes.len() <= 128 * 1024);
            if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..end]);
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':')
                            .filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                            .map(|(_, value)| value.trim().parse().unwrap())
                    })
                    .unwrap();
                if bytes.len() >= end + 4 + length {
                    break serde_json::from_slice::<serde_json::Value>(
                        &bytes[end + 4..end + 4 + length],
                    )
                    .unwrap();
                }
            }
        };
        assert!(bytes.starts_with(b"POST /v1/embeddings"));
        let inputs = request["input"].as_array().unwrap();
        assert!(!inputs.is_empty());
        let mut data = Vec::new();
        for (index, input) in inputs.iter().enumerate() {
            let text = input.as_str().unwrap();
            if preprocessing == "text-full-v2" {
                assert!(text.len() <= 2048);
            }
            observed_bytes += text.len();
            assert!(
                observed_bytes <= expected.len(),
                "no duplicated input bytes"
            );
            received.push(text.to_owned());
            let suffix = if text.contains("suffix") { 1.0 } else { 0.0 };
            data.push(serde_json::json!({"index":index,"embedding":[1.0,suffix,0.0]}));
        }
        requests += 1;
        let body = serde_json::json!({"data":data,"model":"test-embed","usage":{"prompt_tokens":7,"total_tokens":7}}).to_string();
        let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
        socket.write_all(response.as_bytes()).await.unwrap();
    }
    assert_eq!(
        received.concat(),
        expected,
        "every original UTF8 byte arrives in order"
    );
    if preprocessing == "text-v1" {
        assert_eq!(received.len(), 1);
    } else {
        assert!(received.len() > 1);
    }
    requests
}
