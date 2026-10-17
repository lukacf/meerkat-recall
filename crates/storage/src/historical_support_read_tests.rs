//! Retained/corrupt/current certificate controls; synthetic rows are test authority only.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]
use super::*;
use crate::canonical_commit::CanonicalCommitReceipt;
use crate::historical_adoption::{
    HistoricalAdoptionProof, HistoricalCommitProvenance, HistoricalDocumentProof,
    HistoricalUnknownOwnership, HistoricalVectorState,
};
use crate::source_publication::{control_thing, encode_payload, head_key, stable_key};
use crate::source_publication_manifest::{
    manifest_key, SourceDocumentMapping, SourcePublicationManifest,
};
use chrono::Utc;
use elephant_types::provider_binding::SpaceConfigToken;
use elephant_types::publication::PublicationFence;
use elephant_types::records::ingest::IngestMode;
use elephant_types::RecordId;

async fn put<T: Serialize + Sync>(
    client: &StorageClient,
    table: &str,
    key: &str,
    value: &T,
    source: &RecordId,
) {
    let mut row = serde_json::json!({"space_id":"s","source_id":source.to_string(),"payload":encode_payload(value).unwrap()});
    if table == "source_effect_support" {
        let payload = serde_json::to_value(value).unwrap();
        for field in [
            "record_id",
            "source_revision",
            "independent",
            "record_fingerprint",
            "historical_unknown",
        ] {
            row[field] = payload[field].clone();
        }
    }
    if let Some(store) = client.embedded_store() {
        row["id"] = format!("{table}:{key}").into();
        store
            .mutate(|tx| {
                tx.upsert_extension_row(table, row)?;
                Ok(())
            })
            .await
            .unwrap();
    } else {
        client.query_bind_checked("UPSERT $id CONTENT $value;",crate::bindings!{
            "id"=>control_thing(table,key),"value"=>crate::surreal::json_to_surreal(row).unwrap()
        }).await.unwrap();
    }
}

fn manifest(source: &RecordId, record: &RecordId, generation: &str) -> SourcePublicationManifest {
    let doc = RecordId::new_v4(TableName::Doc);
    let revision = RecordId::new_v4(TableName::DocRevision);
    let raw = "a".repeat(64);
    let mapping =
        crate::historical_adoption::document_mapping_sha256([(&doc, raw.as_str())]).unwrap();
    let publication = PublicationFence {
        source_id: source.clone(),
        generation: generation.into(),
        revision_id: format!("revision-{generation}"),
        operation_id: format!("operation-{generation}"),
    };
    SourcePublicationManifest {
        space_id: "s".into(),
        publication: publication.clone(),
        mode: IngestMode::Graph,
        configuration: SpaceConfigToken {
            space_id: "s".into(),
            config_id: RecordId::new_v4(TableName::SpaceConfig).to_string(),
            generation: 1,
            policy_id: "default".into(),
        },
        embedding_profile: None,
        documents: vec![SourceDocumentMapping {
            space_id: "s".into(),
            doc_id: doc.clone(),
            doc_revision_id: revision,
            publication,
        }],
        metadata_artifact_ids: vec![],
        planning_artifact_ids: vec![],
        bundles: vec![],
        embeddings: vec![],
        embedding_work_item_ids: vec![],
        historical_adoption: Some(HistoricalAdoptionProof {
            snapshot_graph: None,
            version: 1,
            source_space_id: "original".into(),
            snapshot_sha256: if generation == "old" { "b" } else { "c" }.repeat(64),
            idmap_sha256: "d".repeat(64),
            plan_id: if generation == "old" { "e" } else { "f" }.repeat(64),
            documents_sha256: "1".repeat(64),
            groups_sha256: "2".repeat(64),
            document_mapping_sha256: mapping,
            external_source_id: "group".into(),
            imported_at: Utc::now(),
            documents: BTreeMap::from([(
                doc,
                HistoricalDocumentProof {
                    original_row_sha256: raw,
                    original_source_id: None,
                    document_fingerprint: stable_key(&["doc"]),
                    revision_fingerprint: stable_key(&["revision"]),
                    original_text_sha256: "3".repeat(64),
                    doc_content_sha256: "4".repeat(64),
                    node_fingerprints: BTreeMap::new(),
                },
            )]),
            graph_fingerprints: BTreeMap::from([(record.clone(), stable_key(&["graph"]))]),
            original_sources: BTreeMap::new(),
            vector_state: HistoricalVectorState::PreservedUnverified,
        }),
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original support reader and storage through old/current receipt corruption, restoration and final valid-support assertion."
)]
async fn retained_old_support_requires_valid_current_and_retained_original_receipts() {
    for embedded in [true, false] {
        let client = if embedded {
            StorageClient::new_embedded().await
        } else {
            StorageClient::new_memory().await
        }
        .unwrap();
        if !embedded {
            for sql in [
                include_str!("../../migrations/sql/031_source_publication.surql"),
                include_str!("../../migrations/sql/035_source_publication_manifest.surql"),
                include_str!("../../migrations/sql/046_source_support_index_fence.surql"),
            ] {
                client.query_checked(sql).await.unwrap();
            }
        }
        let source = RecordId::new_v4(TableName::Source);
        crate::SourceRepository::new(client.clone())
            .create(elephant_types::records::Source::new(
                elephant_types::records::base::BaseFields::new(
                    source.clone(),
                    "s".into(),
                    "fixture".into(),
                ),
                elephant_types::records::SourceType::Manual,
                "fixture".into(),
            ))
            .await
            .unwrap();
        let record = RecordId::new_v4(TableName::Entity);
        let mut supports = Vec::new();
        let mut receipts = Vec::new();
        for generation in ["old", "new"] {
            let manifest = manifest(&source, &record, generation);
            manifest.validate().unwrap();
            let proof = manifest.historical_adoption.as_ref().unwrap();
            let commit = proof.commit_id(&manifest);
            let support = SourceEffectSupport {
                space_id: "s".into(),
                source_id: source.clone(),
                source_revision: manifest.publication.revision_id.clone(),
                record_id: record.clone(),
                independent: false,
                record_fingerprint: stable_key(&["graph"]),
                last_commit_id: Some(commit.clone()),
                historical_unknown: Some(HistoricalUnknownOwnership {
                    snapshot_sha256: proof.snapshot_sha256.clone(),
                    plan_id: proof.plan_id.clone(),
                    commit_id: commit.clone(),
                }),
            };
            let receipt = CanonicalCommitReceipt {
                historical_import: Some(
                    HistoricalCommitProvenance::for_manifest(&manifest).unwrap(),
                ),
                space_id: "s".into(),
                commit_id: commit.clone(),
                input_fingerprint: "fixture".into(),
                plan_fingerprint: manifest.digest().unwrap(),
                replay_output: None,
                source_ids: vec![source.clone()],
                publication_authority: Some(manifest.publication.clone()),
                maintenance_authority: vec![],
                source_id: Some(source.clone()),
                source_revision: Some(support.source_revision.clone()),
                effect_ids: vec![],
                extension_ids: vec![],
                outbox_ids: vec![],
                committed_at: Utc::now(),
            };
            put(
                &client,
                "source_publication_manifest",
                &manifest_key("s", &manifest.publication),
                &manifest,
                &source,
            )
            .await;
            put(
                &client,
                "source_commit_receipt",
                &stable_key(&["s", &commit]),
                &receipt,
                &source,
            )
            .await;
            put(
                &client,
                "source_effect_support",
                &crate::canonical_commit::support_key(
                    "s",
                    &source,
                    &support.source_revision,
                    &record,
                ),
                &support,
                &source,
            )
            .await;
            if generation == "new" {
                let head = SourceHead {
                    space_id: "s".into(),
                    source_id: source.clone(),
                    generation: manifest.publication.generation.clone(),
                    desired_revision: Some(support.source_revision.clone()),
                    published_revision: Some(support.source_revision.clone()),
                    operation_id: manifest.publication.operation_id.clone(),
                    lifecycle: SourceVisibility::Active,
                    updated_at: Utc::now(),
                };
                put(
                    &client,
                    "source_head",
                    &head_key("s", &source),
                    &head,
                    &source,
                )
                .await;
            }
            supports.push(support);
            receipts.push(receipt);
        }
        let reader = ReadEligibility::new(client.clone());
        assert_eq!(
            reader.current_support("s", &record).await.unwrap(),
            vec![supports[1].clone()]
        );
        // Retained old corruption is not hidden just because a current support exists.
        let mut corrupt = receipts[0].clone();
        corrupt.historical_import.as_mut().unwrap().snapshot_sha256 = "0".repeat(64);
        put(
            &client,
            "source_commit_receipt",
            &stable_key(&["s", &corrupt.commit_id]),
            &corrupt,
            &source,
        )
        .await;
        assert!(reader.current_support("s", &record).await.is_err());
        put(
            &client,
            "source_commit_receipt",
            &stable_key(&["s", &receipts[0].commit_id]),
            &receipts[0],
            &source,
        )
        .await;
        let mut corrupt = supports[1].clone();
        corrupt.historical_unknown.as_mut().unwrap().plan_id = "0".repeat(64);
        put(
            &client,
            "source_effect_support",
            &crate::canonical_commit::support_key("s", &source, &corrupt.source_revision, &record),
            &corrupt,
            &source,
        )
        .await;
        assert!(reader.current_support("s", &record).await.is_err());
        put(
            &client,
            "source_effect_support",
            &crate::canonical_commit::support_key(
                "s",
                &source,
                &supports[1].source_revision,
                &record,
            ),
            &supports[1],
            &source,
        )
        .await;
        assert_eq!(
            reader.current_support("s", &record).await.unwrap(),
            vec![supports[1].clone()]
        );
    }
}
