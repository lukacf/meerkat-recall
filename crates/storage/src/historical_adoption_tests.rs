//! Offline contract regressions. No provider or original dataset is needed.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use elephant_types::provider_binding::SpaceConfigToken;
use elephant_types::publication::PublicationFence;
use elephant_types::records::base::BaseFields;
use elephant_types::records::doc::Doc;
use elephant_types::records::ingest::IngestMode;

pub(super) fn document() -> Doc {
    Doc {
        base: BaseFields::new(
            RecordId::new_v4(TableName::Doc),
            "history-test".into(),
            "fixture".into(),
        ),
        title: "Original title".into(),
        text: "Original αβ evidence\n".into(),
        sidecar_text: None,
        source: None,
        media_attachments: vec![],
        rendition_key: "original".into(),
        metadata: serde_json::json!({}),
    }
}
pub(super) fn declaration(doc: &Doc) -> HistoricalDocumentInput {
    HistoricalDocumentInput {
        legacy_doc_id: doc.base.id.clone(),
        external_source_id: "hc-legacy-original".into(),
        original_row_sha256: "a".repeat(64),
        doc_content_sha256: original_document_content_sha256(doc).unwrap(),
        original_text_sha256: sha256(doc.text.as_bytes()),
        original_text_utf8_bytes: doc.text.len(),
        title_sha256: sha256(doc.title.as_bytes()),
        security_sha256: canonical_sha256(&doc.base.security).unwrap(),
        provenance_sha256: canonical_sha256(&doc.base.provenance).unwrap(),
        accepted_request_fingerprint: None,
        accepted_revision: None,
    }
}
pub(super) fn plan(docs: Vec<HistoricalDocumentInput>) -> HistoricalAdoptionInput {
    let group = HistoricalSourceGroup {
        external_source_id: "hc-legacy-original".into(),
        document_mapping_sha256: document_mapping_sha256(
            docs.iter()
                .map(|d| (&d.legacy_doc_id, d.original_row_sha256.as_str())),
        )
        .unwrap(),
        documents: docs,
    };
    let mut ordered: Vec<_> = group.documents.iter().collect();
    ordered.sort_by_key(|d| d.legacy_doc_id.to_string());
    HistoricalAdoptionInput { source_space_id:"history-test".into(), space_id:"history-test".into(),snapshot_sha256:"b".repeat(64),idmap_sha256:"c".repeat(64),plan_id:"d".repeat(64),documents_sha256:jsonl_sha256(ordered).unwrap(),groups_sha256:jsonl_sha256([serde_json::json!({"external_source_id":group.external_source_id,"documents":group.documents.len(),"document_mapping_sha256":group.document_mapping_sha256})]).unwrap(),groups:vec![group],snapshot_groups:vec![] }
}
#[test]
fn historical_input_binds_original_text_policy_provenance_and_no_fake_acceptance() {
    let doc = document();
    let input = declaration(&doc);
    input
        .validate_original_document(&doc, &serde_json::to_value(&doc).unwrap())
        .unwrap();
    let mut changed = doc.clone();
    changed.text.push('!');
    assert!(input
        .validate_original_document(&changed, &serde_json::to_value(&changed).unwrap())
        .is_err());
    let mut changed = doc.clone();
    changed.base.security.level += 1;
    assert!(input
        .validate_original_document(&changed, &serde_json::to_value(&changed).unwrap())
        .is_err());
    let mut changed = doc;
    changed.base.provenance.extractor = Some(elephant_types::records::base::Extractor {
        name: Some("pretend".into()),
        ..Default::default()
    });
    assert!(input
        .validate_original_document(&changed, &serde_json::to_value(&changed).unwrap())
        .is_err());
    let mut fake = input;
    fake.accepted_revision = Some("old-run".into());
    assert!(fake.validate_document(&document()).is_err());
}
#[test]
fn historical_plan_refuses_duplicates_wrong_group_and_changed_aggregate() {
    let doc = document();
    let original = plan(vec![declaration(&doc)]);
    original.validate().unwrap();
    let mut changed = original.clone();
    let duplicate = changed.groups[0].documents[0].clone();
    changed.groups[0].documents.push(duplicate);
    assert!(changed.validate().is_err());
    let mut changed = original.clone();
    changed.groups.push(changed.groups[0].clone());
    assert!(changed.validate().is_err());
    let mut changed = original.clone();
    changed.documents_sha256 = "0".repeat(64);
    assert!(changed.validate().is_err());
    let mut changed = original;
    changed.groups[0].documents[0].external_source_id = "other".into();
    assert!(changed.validate().is_err());
}
#[test]
fn historical_manifest_refuses_duplicate_doc_mapping_and_worker_claims() {
    let first = document();
    let second = document();
    let input = plan(vec![declaration(&first), declaration(&second)]);
    let publication = PublicationFence {
        source_id: RecordId::new_v4(TableName::Source),
        generation: "generation".into(),
        revision_id: "revision".into(),
        operation_id: "operation".into(),
    };
    let documents = input.groups[0]
        .documents
        .iter()
        .map(|d| {
            (
                d.legacy_doc_id.clone(),
                HistoricalDocumentProof {
                    original_row_sha256: d.original_row_sha256.clone(),
                    original_source_id: None,
                    document_fingerprint: stable_key(&["row-proof"]),
                    revision_fingerprint: stable_key(&["revision-proof"]),
                    original_text_sha256: d.original_text_sha256.clone(),
                    doc_content_sha256: d.doc_content_sha256.clone(),
                    node_fingerprints: BTreeMap::new(),
                },
            )
        })
        .collect();
    let mappings = [&first, &second]
        .iter()
        .map(|doc| SourceDocumentMapping {
            space_id: input.space_id.clone(),
            doc_id: doc.base.id.clone(),
            doc_revision_id: RecordId::new_v4(TableName::DocRevision),
            publication: publication.clone(),
        })
        .collect();
    let mut manifest = SourcePublicationManifest {
        space_id: input.space_id.clone(),
        publication,
        mode: IngestMode::Graph,
        configuration: SpaceConfigToken {
            space_id: input.space_id.clone(),
            config_id: RecordId::new_v4(TableName::SpaceConfig).to_string(),
            generation: 1,
            policy_id: "test-policy".into(),
        },
        embedding_profile: None,
        documents: mappings,
        metadata_artifact_ids: vec![],
        planning_artifact_ids: vec![],
        bundles: vec![],
        embeddings: vec![],
        embedding_work_item_ids: vec![],
        historical_adoption: Some(HistoricalAdoptionProof {
            snapshot_graph: None,
            version: 1,
            source_space_id: "history-test".into(),
            snapshot_sha256: input.snapshot_sha256,
            idmap_sha256: input.idmap_sha256,
            plan_id: input.plan_id,
            documents_sha256: input.documents_sha256,
            groups_sha256: input.groups_sha256,
            document_mapping_sha256: input.groups[0].document_mapping_sha256.clone(),
            external_source_id: input.groups[0].external_source_id.clone(),
            imported_at: Utc::now(),
            documents,
            graph_fingerprints: BTreeMap::new(),
            original_sources: BTreeMap::new(),
            vector_state: HistoricalVectorState::PreservedUnverified,
        }),
    };
    manifest.validate().unwrap();
    let original = manifest.clone();
    manifest.documents[1].doc_id = manifest.documents[0].doc_id.clone();
    assert!(manifest.validate().is_err());
    manifest = original;
    manifest
        .embedding_work_item_ids
        .push(RecordId::new_v4(TableName::WorkItem));
    assert!(manifest.validate().is_err());
}

#[test]
fn historical_raw_policy_hash_does_not_invent_omitted_defaults() {
    let doc = document();
    let mut raw = serde_json::to_value(&doc).unwrap();
    raw["security"].as_object_mut().unwrap().remove("labels");
    raw["provenance"].as_object_mut().unwrap().remove("chain");
    let decoded: Doc = serde_json::from_value(raw.clone()).unwrap();
    assert_eq!(doc, decoded);
    let mut input = declaration(&doc);
    input.security_sha256 = canonical_sha256(&raw["security"]).unwrap();
    input.provenance_sha256 = canonical_sha256(&raw["provenance"]).unwrap();
    input.validate_original_document(&doc, &raw).unwrap();
    assert!(input
        .validate_original_document(&doc, &serde_json::to_value(&doc).unwrap())
        .is_err());
    let mut changed = raw;
    changed["provenance"]["extractor"] = serde_json::json!({"name":"invented"});
    assert!(input.validate_original_document(&doc, &changed).is_err());
}

#[test]
fn historical_proof_is_not_transfer_custody() {
    // A deserializable plan alone never constructs PreparedHistoricalAuthority.
    // The asynchronous backend regression covers the real lease and receipt.
    let mut input = plan(vec![declaration(&document())]);
    input.groups[0].documents.clear();
    assert!(input.validate().is_err());
}

#[test]
fn historical_python_planner_raw_hash_interop() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "fixtures/historical-python-raw-documents-v1.json"
    ))
    .unwrap();
    let mut declarations = Vec::new();
    for case in fixture["cases"].as_array().unwrap() {
        let raw = &case["original"];
        let doc: Doc = serde_json::from_value(raw.clone()).unwrap();
        let declared: HistoricalDocumentInput =
            serde_json::from_value(case["declaration"].clone()).unwrap();
        declared.validate_original_document(&doc, raw).unwrap();
        assert_eq!(
            sha256(case["original_lexical"].as_str().unwrap().as_bytes()),
            case["original_lexical_sha256"]
        );
        let mut changed = raw.clone();
        changed["text"] = "changed".into();
        let changed_doc: Doc = serde_json::from_value(changed.clone()).unwrap();
        assert!(declared
            .validate_original_document(&changed_doc, &changed)
            .is_err());
        declarations.push(case["declaration"].clone());
    }
    declarations.sort_by_key(|value| {
        (
            value["external_source_id"].as_str().unwrap().to_owned(),
            value["legacy_doc_id"].as_str().unwrap().to_owned(),
        )
    });
    assert_eq!(
        jsonl_sha256(declarations).unwrap(),
        fixture["documents_sha256"]
    );
}

#[test]
fn historical_typed_links_do_not_treat_metadata_or_text_as_membership() {
    let mut doc = document();
    let bait = RecordId::new(TableName::Source, uuid::Uuid::new_v4());
    doc.text = bait.to_string();
    doc.metadata = serde_json::json!({"source":bait.to_string()});
    assert!(
        historical_evidence_links(&crate::canonical_commit::CanonicalRecord::Doc(Box::new(
            doc.clone()
        )))
        .unwrap()
        .is_empty()
    );
    doc.source = Some(elephant_types::Link::new(bait.clone()));
    assert_eq!(
        historical_evidence_links(&crate::canonical_commit::CanonicalRecord::Doc(Box::new(
            doc
        )))
        .unwrap(),
        BTreeSet::from([bait])
    );
}
