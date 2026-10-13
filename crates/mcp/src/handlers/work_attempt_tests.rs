// Included in work_items::tests; exercises actual public handlers and repositories.
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep lease-token fixture owners through stale-token and original-token assertions."
)]
async fn original_token_contract(storage: StorageClient) {
    use elephant_types::public_error::ErrorCode;
    let repository = WorkItemRepository::new(storage.clone());
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::WorkItem),
        "default".into(),
        "fixture".into(),
    );
    base.security.level = 2;
    let item = repository
        .create(WorkItem {
            base,
            kind: "external-worker-fixture".into(),
            payload: json!({}),
            status: WorkItemStatus::Runnable,
            priority: 1,
            idempotency_key: "external-worker-fixture".into(),
            attempts: 0,
            leased_by: None,
            lease_expires_at: None,
            run_after: None,
            last_error: None,
        })
        .await
        .unwrap();
    let claim = ClaimWorkItemHandler::new(storage.clone());
    let complete = CompleteWorkItemHandler::new(storage.clone());
    let id = item.base.id.to_string();
    let principal = make_test_principal("default", &["read:work_item", "write:work_item"]);
    let low = ToolContext::new(storage.clone(), principal.clone());
    assert!(
        matches!(claim.execute(&low, json!({"id":id,"worker_id":"worker"})).await,
        Err(McpError::Public(error)) if error.error_code == ErrorCode::ForbiddenSpace)
    );
    assert_eq!(
        repository
            .get("default", &item.base.id)
            .await
            .unwrap()
            .unwrap()
            .attempts,
        0
    );
    let mut authorized = principal;
    authorized.clearances[0].max_level = 2;
    let high = ToolContext::new(storage.clone(), authorized.clone());
    let mut write_only = authorized;
    write_only.scopes.remove("read:work_item");
    assert!(
        matches!(claim.execute(&ToolContext::new(storage.clone(), write_only), json!({"id":id,"worker_id":"worker"})).await,
        Err(McpError::Public(error)) if error.error_code == ErrorCode::ForbiddenSpace)
    );
    for duration in [0, u64::MAX, 100_000_000_000_000] {
        assert!(matches!(
            claim
                .execute(
                    &high,
                    json!({"id":id,"worker_id":"worker", "lease_duration_secs":duration})
                )
                .await,
            Err(McpError::InvalidParameters(_))
        ));
    }
    assert_eq!(
        repository
            .get("default", &item.base.id)
            .await
            .unwrap()
            .unwrap()
            .attempts,
        0
    );
    let first = claim
        .execute(&high, json!({"id":id,"worker_id":"worker"}))
        .await
        .unwrap();
    assert_eq!(first["ok"], true);
    let first_token = first["data"]["lease_token"].as_str().unwrap();
    assert_ne!(first_token, "worker");
    assert_eq!(
        repository
            .get("default", &item.base.id)
            .await
            .unwrap()
            .unwrap()
            .leased_by
            .as_deref(),
        Some(first_token)
    );
    for request in [
        json!({"id":id}),
        json!({"id":id,"lease_token":null}),
        json!({"id":id,"lease_token":""}),
        json!({"id":id,"lease_token":"  "}),
    ] {
        assert!(matches!(
            complete.execute(&high, request).await,
            Err(McpError::InvalidParameters(_))
        ));
    }
    assert!(
        matches!(complete.execute(&low, json!({"id":id,"lease_token":first_token})).await,
        Err(McpError::Public(error)) if error.error_code == ErrorCode::ForbiddenSpace)
    );
    assert!(
        matches!(complete.execute(&high, json!({"id":id,"lease_token":"worker"})).await,
        Err(McpError::Public(error)) if error.error_code == ErrorCode::ConflictGeneration && !error.retryable)
    );
    repository
        .requeue_with_delay("default", &item.base.id, first_token, 0, None)
        .await
        .unwrap()
        .unwrap();
    let second = claim
        .execute(&high, json!({"id":id,"worker_id":"worker"}))
        .await
        .unwrap();
    let second_token = second["data"]["lease_token"].as_str().unwrap();
    assert_ne!(first_token, second_token);
    assert!(
        matches!(complete.execute(&high, json!({"id":id,"lease_token":first_token})).await,
        Err(McpError::Public(error)) if error.error_code == ErrorCode::ConflictGeneration && !error.retryable)
    );
    let successor = repository
        .get("default", &item.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(successor.status, WorkItemStatus::Running);
    assert_eq!(successor.leased_by.as_deref(), Some(second_token));
    let response = complete
        .execute(&high, json!({"id":id,"lease_token":second_token}))
        .await
        .unwrap();
    assert_eq!(response["ok"], true);
    assert_eq!(
        repository
            .get("default", &item.base.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        WorkItemStatus::Done
    );
}

#[tokio::test]
async fn embedded_public_completion_requires_original_token_and_actual_row_authority() {
    original_token_contract(StorageClient::new_embedded().await.unwrap()).await;
}
#[tokio::test]
async fn surreal_public_completion_requires_original_token_and_actual_row_authority() {
    original_token_contract(StorageClient::new_memory().await.unwrap()).await;
}
