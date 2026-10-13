// Included in governed::tests. Uses the real HTTP decoder against a local fixture.
#[derive(Debug, Default)]
struct CompletionObserver(std::sync::Mutex<Vec<(String, &'static str)>>);
#[async_trait]
impl ProviderDispatchObserver for CompletionObserver {
    async fn before_dispatch(&self, call: &str, _: &ModelBindingProvenance) -> EmbeddingResult<()> {
        self.0.lock().unwrap().push((call.into(), "start"));
        Ok(())
    }
    async fn observe_usage(
        &self,
        call: &str,
        _: &ModelBindingProvenance,
        input: Option<u64>,
        output: Option<u64>,
    ) -> EmbeddingResult<()> {
        assert_eq!((input, output), (Some(7), Some(0)));
        self.0.lock().unwrap().push((call.into(), "usage"));
        Ok(())
    }
    async fn dispatch_finished(
        &self,
        call: &str,
        _: &ModelBindingProvenance,
    ) -> EmbeddingResult<()> {
        self.0.lock().unwrap().push((call.into(), "complete"));
        Ok(())
    }
}

#[tokio::test]
#[cfg_attr(
    feature = "fastembed",
    expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original provider alive through decoder, server and receipt assertions."
    )
)]
async fn physical_embedding_completion_requires_decoded_response() {
    for complete in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let approval = Arc::new(approval(format!(
            "http://{}/v1",
            listener.local_addr().unwrap()
        )));
        let binding = approval
            .space
            .authorize("a", ModelOperation::QueryEmbedding, None)
            .unwrap();
        let observer = Arc::new(CompletionObserver::default());
        let provider = GovernedEmbeddingProvider::new(
            EmbeddingDispatch::new(approval.clone(), Arc::new(Secret), binding)
                .with_observer(Some(observer.clone())),
        )
        .unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            loop {
                let mut chunk = [0; 4096];
                let count = socket.read(&mut chunk).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&chunk[..count]);
                assert!(bytes.len() <= 64 * 1024);
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
                        break;
                    }
                }
            }
            assert!(bytes.starts_with(b"POST /v1/embeddings"));
            let body = if complete {
                r#"{"data":[{"index":0,"embedding":[0.5,0.5,0.5]}],"model":"test-embed","usage":{"prompt_tokens":7,"total_tokens":7}}"#
            } else {
                r#"{"data":["#
            };
            let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            provider.embed(&["local fixture".into()]),
        )
        .await
        .unwrap();
        server.await.unwrap();
        assert_eq!(result.is_ok(), complete);
        let events = observer.0.lock().unwrap();
        assert_eq!(
            events.iter().map(|(_, event)| *event).collect::<Vec<_>>(),
            if complete {
                vec!["start", "usage", "complete"]
            } else {
                vec!["start"]
            }
        );
        assert!(events.iter().all(|(call, _)| call == &events[0].0));
        drop(events);
    }
}
