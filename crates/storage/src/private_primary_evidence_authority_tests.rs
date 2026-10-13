#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use crate::canonical_commit::CanonicalCommitReceipt;
use elephant_types::records::{base::BaseFields, SourceType};

#[test]
#[allow(clippy::too_many_lines)]
#[expect(
    clippy::unreadable_literal,
    reason = "Preserve the original small-number JSON fixture used to verify exact canonical row reconstruction."
)]
fn compact_inventory_reconstructs_all_rows_and_hashes_within_original_limits() {
    let mut packet = Packet::default();
    let entities = (0..MAX_ROWS)
        .map(|i| {
            json!({
                "id":format!("entity:{i:04}"),
                "space_id":SPACES[0],
                "deleted_at":if i % 3 == 0 { json!("2026-10-07T00:00:00Z") } else { Value::Null },
                "superseded":i % 5 == 0,
                "subjects_state":"unknown",
                "original_text":"Quoted \"source\" \\ café 水",
                "small_number":0.0000001,
            })
        })
        .collect::<Vec<_>>();
    let calls = (0..MAX_PROVIDER_RECEIPTS)
        .map(|i| json!({"call_id":format!("original-call-{i:04}"),"usage_complete":false}))
        .collect::<Vec<_>>();
    let mut unordered_entities = entities.clone();
    unordered_entities.reverse();
    insert_table(&mut packet, "entity", unordered_entities).unwrap();
    insert_table(&mut packet, "provider_call_receipt", calls.clone()).unwrap();
    insert_table(&mut packet, "rel", Vec::new()).unwrap();
    let inventory = inventory(&packet).unwrap();
    let expected_rows = BTreeMap::from([
        ("entity", entities),
        ("provider_call_receipt", calls),
        ("rel", Vec::new()),
    ]);
    let mut expected_manifests = BTreeMap::new();
    for (table, rows) in &expected_rows {
        let entries = rows
            .iter()
            .map(|row| {
                json!({
                    "id":row.get("id").or_else(|| row.get("call_id")).unwrap(),
                    "sha256":digest(&encode(row).unwrap()),
                })
            })
            .collect::<Vec<_>>();
        expected_manifests.insert(*table, entries);
    }
    // The former duplicate inline manifest cannot fit the reply even though
    // every selected row fits the documented original row/page/byte ceilings.
    assert!(encode(&expected_manifests["entity"]).unwrap().len() > MAX_REPLY_BYTES);
    packet.evidence = json!({"complete":true,"inventory":inventory});
    let request = Request {
        format: REQUEST_FORMAT.into(),
        request_id: "a".repeat(64),
        run_nonce: "compact-original-fixture".into(),
        backend: "elephant-db".into(),
        space: SPACES[0].into(),
        kind: "current_canonical".into(),
        selector: json!({}),
    };
    let prepared = prepare_packet(
        &request,
        &encode(&request).unwrap(),
        packet,
        None,
        &Allocation::default(),
    )
    .unwrap();
    assert_eq!(prepared.rows, MAX_ROWS);
    assert_eq!(prepared.calls.len(), MAX_PROVIDER_RECEIPTS);
    assert!(prepared.reply.len() <= MAX_REPLY_BYTES);
    assert!(prepared.bytes <= MAX_TOTAL_BYTES);
    assert!(prepared.pages.len() <= MAX_PAGES);
    let reply: Value = serde_json::from_slice(&prepared.reply).unwrap();
    assert_eq!(reply["status"], "complete");
    let mut reconstructed: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for (index, (name, raw)) in prepared.pages.iter().enumerate() {
        let descriptor = &reply["pages"][index];
        assert_eq!(descriptor["name"], json!(name));
        assert_eq!(descriptor["sha256"], digest(raw));
        assert_eq!(descriptor["bytes"], raw.len());
        assert!(raw.len() <= MAX_PAGE_BYTES);
        let rows: Vec<Value> = serde_json::from_slice(raw).unwrap();
        assert_eq!(descriptor["rows"], rows.len());
        assert!(rows.len() <= MAX_PAGE_ROWS);
        reconstructed
            .entry(descriptor["table"].as_str().unwrap().into())
            .or_default()
            .extend(rows);
    }
    for (table, original) in &expected_rows {
        let rows = reconstructed.entry((*table).into()).or_default();
        assert_eq!(rows.as_slice(), original.as_slice());
        let manifest = &reply["evidence"]["inventory"][*table];
        assert_eq!(manifest["count"], rows.len());
        assert_eq!(manifest["table_sha256"], digest(&encode(rows).unwrap()));
        assert_eq!(manifest["exhausted"], true);
        assert_eq!(
            manifest["rows"],
            json!({
                "format":"recall-private-primary-row-manifest/v1",
                "source":"original_table_pages",
                "encoding":"canonical_json_utf8",
                "sha256":digest(&encode(&expected_manifests[table]).unwrap()),
            })
        );
    }
    let original_hash = reply["evidence"]["inventory"]["entity"]["table_sha256"]
        .as_str()
        .unwrap();
    let original = &expected_rows["entity"];
    let mut changed = original.clone();
    changed[0]["superseded"] = json!(false);
    assert_ne!(digest(&encode(&changed).unwrap()), original_hash);
    let mut missing = original.clone();
    missing.remove(0);
    assert_ne!(digest(&encode(&missing).unwrap()), original_hash);
    let mut reordered = original.clone();
    reordered.swap(0, 1);
    assert_ne!(digest(&encode(&reordered).unwrap()), original_hash);
    let mut duplicated = original.clone();
    duplicated[1] = duplicated[0].clone();
    assert_ne!(digest(&encode(&duplicated).unwrap()), original_hash);
}

#[test]
fn compact_inventory_rejects_duplicate_and_missing_physical_identities() {
    for rows in [
        vec![
            json!({"id":"entity:one"}),
            json!({"id":"entity:one","deleted_at":"original"}),
        ],
        vec![json!({"space_id":SPACES[0]})],
    ] {
        let packet = Packet {
            evidence: Value::Null,
            tables: BTreeMap::from([("entity".into(), rows)]),
        };
        assert!(inventory(&packet).is_err());
    }
}

async fn client(embedded: bool) -> StorageClient {
    if embedded {
        StorageClient::new_embedded().await.unwrap()
    } else {
        StorageClient::new_memory().await.unwrap()
    }
}
#[expect(
    clippy::future_not_send,
    reason = "This serialization fixture is awaited in its original task and does not require a Send type parameter."
)]
async fn control<T: Serialize>(
    client: &StorageClient,
    table: &str,
    key: &str,
    space: &str,
    row: &T,
) {
    let payload = serde_json::to_string(row).unwrap();
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| {
                tx.upsert_extension_row(
                    table,
                    json!({"id":format!("{table}:{key}"),"space_id":space,"payload":payload}),
                )?;
                Ok(())
            })
            .await
            .unwrap();
    } else {
        client.query_bind_checked("UPSERT $id SET space_id=$space,payload=$payload;",crate::bindings!{"id"=>crate::source_publication::control_thing(table,key),"space"=>space,"payload"=>payload}).await.unwrap();
    }
}
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn authority(embedded: bool) {
    let client = client(embedded).await;
    let space = "private-original";
    let mut source = Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            space.into(),
            "fixture".into(),
        ),
        SourceType::Manual,
        "original".into(),
    );
    source.external_id = Some("home/visit".into());
    SourceRepository::new(client.clone())
        .create(source.clone())
        .await
        .unwrap();
    let selection = PublicationSelection {
        source_key: "home/visit".into(),
        publication: PublicationFence {
            source_id: source.base.id.clone(),
            generation: "original-generation".into(),
            revision_id: "visit-r1".into(),
            operation_id: "original-operation".into(),
        },
        doc_id: RecordId::new_v4(TableName::Doc),
        doc_revision_id: RecordId::new_v4(TableName::DocRevision),
    };
    let mut head = SourceHead {
        space_id: space.into(),
        source_id: source.base.id.clone(),
        generation: selection.publication.generation.clone(),
        desired_revision: Some("visit-r1".into()),
        published_revision: Some("visit-r1".into()),
        operation_id: selection.publication.operation_id.clone(),
        lifecycle: SourceVisibility::Active,
        updated_at: Utc::now(),
    };
    let key = crate::source_publication::head_key(space, &source.base.id);
    control(&client, "source_head", &key, space, &head).await;
    let receipt = CanonicalCommitReceipt {
        space_id: space.into(),
        commit_id: "original-commit".into(),
        input_fingerprint: "input".into(),
        plan_fingerprint: "plan".into(),
        replay_output: Some(json!({"private_original":"never exported"})),
        source_ids: vec![source.base.id.clone()],
        publication_authority: Some(selection.publication.clone()),
        maintenance_authority: vec![],
        source_id: Some(source.base.id.clone()),
        source_revision: Some("visit-r1".into()),
        effect_ids: vec![],
        extension_ids: vec![],
        outbox_ids: vec![],
        committed_at: Utc::now(),
    };
    control(
        &client,
        "source_commit_receipt",
        &stable_key(&[space, &receipt.commit_id]),
        space,
        &receipt,
    )
    .await;
    let repository = CanonicalRepository::new(client.clone());
    assert!(current_source(&client, space, &selection).await.is_ok());
    let original = client
        .with_activity(repository.receipt_read_only(space, &receipt.commit_id))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let exported = receipt_projection(&original).unwrap();
    assert!(exported["canonical_receipt"].get("replay_output").is_none());
    assert_eq!(exported["replay_output_omission"]["present"], true);
    assert!(!encode(&exported)
        .unwrap()
        .windows(b"never exported".len())
        .any(|window| window == b"never exported"));
    // A newly desired revision already refuses the private original capture,
    // even before the previous published revision is replaced.
    head.desired_revision = Some("visit-r2".into());
    head.generation = "successor-generation".into();
    control(&client, "source_head", &key, space, &head).await;
    assert!(current_source(&client, space, &selection).await.is_err());
    assert!(repository
        .receipt_read_only(space, &receipt.commit_id)
        .await
        .is_err());
    head.published_revision = Some("visit-r2".into());
    control(&client, "source_head", &key, space, &head).await;
    assert!(repository
        .receipt_read_only(space, &receipt.commit_id)
        .await
        .is_err());
    head.lifecycle = SourceVisibility::Erased;
    control(&client, "source_head", &key, space, &head).await;
    assert!(current_source(&client, space, &selection).await.is_err());
    assert!(repository
        .receipt_read_only("foreign-space", &receipt.commit_id)
        .await
        .unwrap()
        .is_none());
    client.block_on_incomplete_erasure();
    assert!(client
        .with_activity(repository.receipt_read_only(space, &receipt.commit_id))
        .await
        .is_err());
}
#[tokio::test]
async fn embedded_original_receipt_rejects_supersession_erasure_and_recovery_barrier() {
    authority(true).await;
}
#[tokio::test]
async fn surreal_original_receipt_rejects_supersession_erasure_and_recovery_barrier() {
    authority(false).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn inventory_page(embedded: bool) {
    let client = client(embedded).await;
    let repository = SourceRepository::new(client.clone());
    let mut expected = BTreeSet::new();
    for i in 0..106 {
        let space = if i == 105 { "foreign" } else { "selected" };
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                space.into(),
                "fixture".into(),
            ),
            SourceType::Manual,
            format!("input-{i}"),
        );
        if i == 4 {
            source.base.deleted_at = Some(Utc::now());
        }
        if i < 105 {
            expected.insert(source.base.id.clone());
        }
        repository.create(source).await.unwrap();
    }
    let first = typed_ids(&client, TableName::Source, "selected", None)
        .await
        .unwrap();
    assert_eq!(first.len(), 100);
    let second = typed_ids(&client, TableName::Source, "selected", first.last())
        .await
        .unwrap();
    assert_eq!(second.len(), 5);
    let end = typed_ids(&client, TableName::Source, "selected", second.last())
        .await
        .unwrap();
    assert!(end.is_empty());
    assert_eq!(
        first.into_iter().chain(second).collect::<BTreeSet<_>>(),
        expected
    );
}
#[tokio::test]
async fn embedded_inventory_pages_include_deleted_ids_without_foreign_rows() {
    inventory_page(true).await;
}
#[tokio::test]
async fn surreal_inventory_pages_include_deleted_ids_without_foreign_rows() {
    inventory_page(false).await;
}

#[test]
fn completion_requires_selected_document_and_revision_not_just_nonempty_maps() {
    let selection = PublicationSelection {
        source_key: "home/visit".into(),
        publication: PublicationFence {
            source_id: RecordId::new_v4(TableName::Source),
            generation: "original-generation".into(),
            revision_id: "visit-r1".into(),
            operation_id: "original-operation".into(),
        },
        doc_id: RecordId::new_v4(TableName::Doc),
        doc_revision_id: RecordId::new_v4(TableName::DocRevision),
    };
    let mut completion = SourcePublicationCompletion {
        space_id: SPACES[0].into(),
        publication: selection.publication.clone(),
        manifest_digest: "original-manifest".into(),
        commit_id: "original-commit".into(),
        rendered_revision_fingerprints: BTreeMap::new(),
        document_fingerprints: BTreeMap::new(),
        accepted_source_fingerprint: Some("original-source-fingerprint".into()),
        published_progress: None,
        completed_at: Utc::now(),
    };
    // An otherwise identified certificate with no rendered proof is incomplete.
    assert_eq!(
        verify_selected_render_membership(&completion, &selection)
            .unwrap_err()
            .0,
        "selected_render_certificate_missing"
    );
    completion
        .document_fingerprints
        .insert(selection.doc_id.clone(), "document-fingerprint".into());
    assert!(verify_selected_render_membership(&completion, &selection).is_err());
    // Equal nonzero counts cannot substitute another document revision.
    let other_revision = RecordId::new_v4(TableName::DocRevision);
    completion
        .rendered_revision_fingerprints
        .insert(other_revision.clone(), "other-revision".into());
    assert!(verify_selected_render_membership(&completion, &selection).is_err());
    completion
        .rendered_revision_fingerprints
        .remove(&other_revision);
    completion.rendered_revision_fingerprints.insert(
        selection.doc_revision_id.clone(),
        "revision-fingerprint".into(),
    );
    assert!(verify_selected_render_membership(&completion, &selection).is_ok());
    completion.document_fingerprints.clear();
    completion
        .document_fingerprints
        .insert(RecordId::new_v4(TableName::Doc), "other-document".into());
    assert!(verify_selected_render_membership(&completion, &selection).is_err());
}
