#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
fn capability() -> Capability {
    Capability {
        format: CAPABILITY_FORMAT.into(),
        run_nonce: "source33-original-fixture".into(),
        backend: "elephant-db".into(),
        corpus_sha256: CORPUS_SHA256.into(),
        expires_at: Utc::now() + chrono::Duration::hours(1),
        directory: "/private/original".into(),
        source_revisions: serde_json::from_str(include_str!(
            "private_primary_evidence_allowlist.json"
        ))
        .unwrap(),
    }
}
fn request() -> Request {
    let mut request = Request {
        format: REQUEST_FORMAT.into(),
        request_id: String::new(),
        run_nonce: capability().run_nonce,
        backend: "elephant-db".into(),
        space: SPACES[0].into(),
        kind: "original_maintenance".into(),
        selector: json!({"job_id":"reprocess_job:11111111-1111-4111-8111-111111111111"}),
    };
    let mut v = serde_json::to_value(&request).unwrap();
    v.as_object_mut().unwrap().remove("request_id");
    request.request_id = digest(&encode(&v).unwrap());
    request
}
#[test]
fn activation_is_disabled_only_when_both_variables_are_absent() {
    assert!(PrivatePrimaryEvidence::from_activation(None, None)
        .unwrap()
        .is_none());
    assert!(PrivatePrimaryEvidence::from_activation(Some("/private/cap".into()), None).is_err());
    assert!(PrivatePrimaryEvidence::from_activation(None, Some("0".repeat(64).into())).is_err());
}
#[test]
fn capability_rejects_expiry_foreign_scope_and_changed_original_hash() {
    let original = capability();
    assert!(original.verify(Utc::now()).is_ok());
    let mut cap = original.clone();
    cap.expires_at = Utc::now() - chrono::Duration::seconds(1);
    assert!(cap.verify(Utc::now()).is_err());
    let mut cap = original.clone();
    cap.source_revisions[0].space = "foreign".into();
    assert!(cap.verify(Utc::now()).is_err());
    let mut cap = original;
    cap.source_revisions[0].input_sha256 = "0".repeat(64);
    assert!(cap.verify(Utc::now()).is_err());
}
#[test]
fn request_rejects_wrong_nonce_backend_scope_digest_and_path() {
    let cap = capability();
    let original = request();
    assert!(original.verify(&cap, &original.request_id).is_ok());
    for field in ["nonce", "backend", "space", "digest"] {
        let mut row = original.clone();
        match field {
            "nonce" => row.run_nonce = "wrong-original-nonce".into(),
            "backend" => row.backend = "surreal-db".into(),
            "space" => row.space = "foreign".into(),
            _ => row.request_id = "0".repeat(64),
        }
        assert!(row.verify(&cap, &row.request_id).is_err());
    }
    assert!(original.verify(&cap, "../../original").is_err());
}
#[test]
fn nested_duplicate_fields_are_rejected_before_selector_decoding() {
    assert!(strict_json::<Value>(br#"{"selector":{"job_id":"first","job_id":"second"}}"#).is_err());
    assert!(strict_json::<Value>(br#"{"a":1,"a":2}"#).is_err());
    assert_eq!(
        strict_json::<Value>(br#"{"a":[true,null,2,"quoted"]}"#).unwrap(),
        json!({"a":[true,null,2,"quoted"]})
    );
}
#[test]
fn page_boundaries_keep_all_rows_and_exact_hashes() {
    let request = request();
    let raw = encode(&request).unwrap();
    let rows = (0..129)
        .map(|i| json!({"id":format!("entity:{i:04}"),"space_id":SPACES[0]}))
        .collect::<Vec<_>>();
    let p = prepare_packet(
        &request,
        &raw,
        Packet {
            evidence: json!({"complete":true}),
            tables: BTreeMap::from([("entity".into(), rows.clone())]),
        },
        None,
        &Allocation::default(),
    )
    .unwrap();
    assert_eq!(p.pages.len(), 2);
    let reply: Value = serde_json::from_slice(&p.reply).unwrap();
    let mut combined = Vec::new();
    for (index, (name, bytes)) in p.pages.iter().enumerate() {
        assert_eq!(reply["pages"][index]["name"], json!(name));
        assert_eq!(reply["pages"][index]["sha256"], digest(bytes));
        combined.extend(serde_json::from_slice::<Vec<Value>>(bytes).unwrap());
    }
    assert_eq!(combined, rows);
    assert_eq!(p.rows, 129);
}
#[test]
fn overflow_is_refused_without_truncation_or_complete_reply() {
    let request = request();
    let raw = encode(&request).unwrap();
    let packet = Packet {
        evidence: json!({}),
        tables: BTreeMap::from([(
            "entity".into(),
            vec![json!({"id":"entity:one","text":"x".repeat(MAX_PAGE_BYTES)})],
        )]),
    };
    assert!(prepare_packet(&request, &raw, packet, None, &Allocation::default()).is_err());
    let allocation = Allocation {
        bytes: MAX_TOTAL_BYTES,
        ..Default::default()
    };
    assert!(prepare_packet(&request, &raw, Packet::default(), None, &allocation).is_err());
    let incomplete = prepare_packet(
        &request,
        &raw,
        Packet {
            evidence: json!({"complete":false}),
            ..Default::default()
        },
        Some("capture_timeout"),
        &Allocation::default(),
    )
    .unwrap();
    let reply: Value = serde_json::from_slice(&incomplete.reply).unwrap();
    assert_eq!(reply["status"], "incomplete");
    assert_eq!(reply["error_code"], "capture_timeout");
    assert_eq!(reply["pages"], json!([]));
}
#[test]
fn missing_usage_is_preserved_and_provider_count_is_unique() {
    let request = request();
    let raw = encode(&request).unwrap();
    let call = json!({"call_id":"original-call","input_tokens":null,"output_tokens":null,"usage_complete":false});
    let packet = Packet {
        evidence: json!({}),
        tables: BTreeMap::from([("provider_call_receipt".into(), vec![call.clone()])]),
    };
    let allocation = Allocation {
        calls: BTreeSet::from(["original-call".into()]),
        ..Default::default()
    };
    let prepared = prepare_packet(&request, &raw, packet, None, &allocation).unwrap();
    assert_eq!(prepared.rows, 0);
    assert_eq!(prepared.calls.len(), 1);
    assert_eq!(
        serde_json::from_slice::<Value>(&prepared.pages[0].1).unwrap(),
        json!([call])
    );
}

#[test]
fn allocation_refusals_identify_each_original_bound_without_resetting_usage() {
    let request = request();
    let raw = encode(&request).unwrap();
    let one_row = || Packet {
        evidence: json!({"complete":true}),
        tables: BTreeMap::from([("entity".into(), vec![json!({"id":"entity:one"})])]),
    };
    let one_call = || Packet {
        evidence: json!({"complete":true}),
        tables: BTreeMap::from([(
            "provider_call_receipt".into(),
            vec![json!({"call_id":"new-original-call","usage_complete":false})],
        )]),
    };
    let reply_overflow = Packet {
        evidence: json!({"complete":true,"untruncated":"x".repeat(MAX_REPLY_BYTES)}),
        ..Default::default()
    };
    assert_eq!(
        prepare_packet(&request, &raw, reply_overflow, None, &Allocation::default())
            .err()
            .unwrap()
            .0,
        "capture_reply_byte_limit"
    );
    let first = prepare_packet(&request, &raw, one_row(), None, &Allocation::default()).unwrap();
    let exact = Allocation {
        bytes: MAX_TOTAL_BYTES - first.bytes,
        rows: MAX_ROWS - 1,
        calls: BTreeSet::new(),
        ..Default::default()
    };
    assert!(prepare_packet(&request, &raw, one_row(), None, &exact).is_ok());
    let exhausted_bytes = Allocation {
        bytes: exact.bytes + 1,
        ..Default::default()
    };
    assert_eq!(
        prepare_packet(&request, &raw, one_row(), None, &exhausted_bytes)
            .err()
            .unwrap()
            .0,
        "capture_total_byte_limit"
    );
    // A newly written physical page still consumes its rows, even if the row
    // was seen before. Only an exact row from a retained maintenance page avoids a write.
    let exhausted_rows = Allocation {
        rows: MAX_ROWS,
        ..Default::default()
    };
    assert_eq!(
        prepare_packet(&request, &raw, one_row(), None, &exhausted_rows)
            .err()
            .unwrap()
            .0,
        "capture_row_limit"
    );
    let mut calls = Allocation {
        calls: (0..MAX_PROVIDER_RECEIPTS - 1)
            .map(|i| format!("earlier-call-{i}"))
            .collect(),
        ..Default::default()
    };
    assert!(prepare_packet(&request, &raw, one_call(), None, &calls).is_ok());
    calls.calls.insert("last-earlier-call".into());
    assert_eq!(
        prepare_packet(&request, &raw, one_call(), None, &calls)
            .err()
            .unwrap()
            .0,
        "capture_provider_receipt_limit"
    );
    assert_eq!(calls.calls.len(), MAX_PROVIDER_RECEIPTS);
    assert_eq!(exact.bytes + first.bytes, MAX_TOTAL_BYTES);
    assert_eq!(exact.rows + first.rows, MAX_ROWS);
}
#[test]
fn frozen_selection_contains_both_visit_revisions_in_original_order() {
    let cap = capability();
    let visits = cap
        .source_revisions
        .iter()
        .filter(|r| r.source_key == "home/visit")
        .map(|r| r.revision.as_str())
        .collect::<Vec<_>>();
    assert_eq!(visits, vec!["visit-r1", "visit-r2"]);
    assert_eq!(cap.source_revisions.len(), 28);
    assert_eq!(
        cap.source_revisions
            .iter()
            .filter(|r| r.space == SPACES[0])
            .count(),
        13
    );
}

#[tokio::test]
async fn timeout_cancels_original_capture_without_starting_a_second_attempt() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    struct Dropped(Arc<AtomicUsize>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let starts = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let capture = async {
        starts.fetch_add(1, Ordering::SeqCst);
        let _guard = Dropped(drops.clone());
        std::future::pending::<Result<()>>().await
    };
    let result = capture_once(std::time::Duration::from_millis(1), capture).await;
    assert_eq!(result.unwrap_err().0, "capture_timeout");
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn blocked_storage_publishes_one_incomplete_original_then_refuses_reuse() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    for name in ["requests", "responses"] {
        let p = root.path().join(name);
        std::fs::create_dir(&p).unwrap();
        std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let mut cap = capability();
    cap.directory = root.path().to_owned();
    let capraw = encode(&cap).unwrap();
    let path = root.path().join("capability.json");
    std::fs::write(&path, &capraw).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let mut worker = PrivatePrimaryEvidence::from_activation(
        Some(path.into_os_string()),
        Some(digest(&capraw).into()),
    )
    .unwrap()
    .unwrap();
    let request = request();
    let bytes = encode(&request).unwrap();
    let requestpath = root
        .path()
        .join("requests")
        .join(format!("{}.json", request.request_id));
    std::fs::write(&requestpath, &bytes).unwrap();
    std::fs::set_permissions(&requestpath, std::fs::Permissions::from_mode(0o600)).unwrap();
    let client = StorageClient::new_embedded().await.unwrap();
    client.block_on_incomplete_erasure();
    assert!(worker.poll(&client).await.is_err());
    let replypath = root
        .path()
        .join("responses")
        .join(format!("{}.json", request.request_id));
    let original = std::fs::read(&replypath).unwrap();
    let reply: Value = serde_json::from_slice(&original).unwrap();
    assert_eq!(reply["status"], "incomplete");
    assert_eq!(reply["error_code"], "storage_recovery_not_ready");
    assert_eq!(reply["pages"], json!([]));
    assert!(worker.poll(&client).await.is_err());
    assert_eq!(std::fs::read(replypath).unwrap(), original);
    assert!(worker.publications.is_empty());
}
