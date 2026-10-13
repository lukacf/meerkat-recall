//! Independent D expectations composed with F's actual accepted-contact worker
//! fixture. F includes this child from `source_contact_finalization_tests.rs`.
//! Raw private-control edits below are explicit corruption faults, never setup
//! for a positive proof. Every fault starts from a real verified publication.

use super::*;
use elephant_storage::canonical_commit::SourceEffectSupport;
use elephant_storage::public_mutation::PublicMutation;
use elephant_types::records::Rel;

#[expect(
    clippy::format_collect,
    reason = "Retain the exact independent contact control key construction"
)]
fn control_key(parts: &[&str]) -> String {
    let original = parts
        .iter()
        .map(|part| format!("{}:{part}", part.len()))
        .collect::<String>();
    uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, original.as_bytes()).to_string()
}

// The kernel returns fully qualified keys and caps each page at 101 rows.
// Traverse the original complete table within the independent 256-row bound.
async fn embedded_controls(
    storage: &StorageClient,
    table: &str,
) -> Vec<(String, serde_json::Value)> {
    let store = storage.embedded_store().unwrap();
    let mut rows = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..3 {
        let page = store
            .extension_rows_page(table, SPACE, cursor.as_deref(), 101)
            .await
            .unwrap();
        assert!(page.len() <= 101);
        assert!(page.windows(2).all(|pair| pair[0].0 < pair[1].0));
        assert!(page
            .first()
            .is_none_or(|(key, _)| cursor.as_ref().is_none_or(|prior| key > prior)));
        let complete = page.len() < 101;
        cursor = page.last().map(|(key, _)| key.clone());
        rows.extend(page);
        assert!(
            rows.len() <= 256,
            "independent private-control fixture bound"
        );
        if complete {
            return rows;
        }
    }
    panic!("bounded complete private-control inventory was not obtained");
}

async fn control_payload(storage: &StorageClient, table: &str, key: &str) -> String {
    assert!(matches!(
        table,
        "source_effect_support" | "source_commit_receipt"
    ));
    if storage.embedded_store().is_some() {
        let rows = embedded_controls(storage, table).await;
        let full_key = format!("{table}:{key}");
        let selected: Vec<_> = rows.iter().filter(|(id, _)| id == &full_key).collect();
        assert_eq!(selected.len(), 1, "original private control must exist");
        return selected[0].1["payload"].as_str().unwrap().to_owned();
    }
    let mut response = storage
        .query_bind_checked(
            "SELECT VALUE payload FROM type::thing($table,$key)",
            std::collections::BTreeMap::from([
                ("table", table.to_owned()),
                ("key", key.to_owned()),
            ]),
        )
        .await
        .unwrap();
    let rows: Vec<String> = response.take(0).unwrap();
    assert_eq!(rows.len(), 1, "original native private control must exist");
    rows[0].clone()
}

async fn corrupt_control_payload(
    storage: &StorageClient,
    table: &str,
    key: &str,
    original: &str,
    replacement: &str,
) {
    assert_eq!(control_payload(storage, table, key).await, original);
    if let Some(store) = storage.embedded_store() {
        let full_key = format!("{table}:{key}");
        let mut row = embedded_controls(storage, table)
            .await
            .into_iter()
            .find(|(id, _)| id == &full_key)
            .unwrap()
            .1;
        row["payload"] = serde_json::json!(replacement);
        store
            .mutate(|tx| Ok(tx.upsert_extension_row(table, row.clone())?))
            .await
            .unwrap();
    } else {
        storage
            .query_bind_checked(
                "UPDATE type::thing($table,$key) SET payload=$replacement WHERE payload=$original",
                std::collections::BTreeMap::from([
                    ("table", table.to_owned()),
                    ("key", key.to_owned()),
                    ("original", original.to_owned()),
                    ("replacement", replacement.to_owned()),
                ]),
            )
            .await
            .unwrap();
    }
    assert_eq!(control_payload(storage, table, key).await, replacement);
}

async fn source_contact_rel(storage: &StorageClient, source: &RecordId) -> Rel {
    let people = EntityRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap();
    let people: Vec<_> = people
        .iter()
        .filter(|row| row.display_name == CONTACT)
        .collect();
    assert_eq!(people.len(), 1);
    let relations = RelRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap();
    let relations: Vec<_> = relations
        .into_iter()
        .filter(|row| {
            row.subject.id() == &people[0].base.id
                && row.rel_type == "works_at"
                && row.base.provenance.sources.contains(source)
        })
        .collect();
    assert_eq!(relations.len(), 1);
    let result = relations.into_iter().next().unwrap();
    assert!(
        result.evidence.is_empty(),
        "exercise actual contact seal, not assertion evidence"
    );
    result
}

#[derive(Clone, Copy, Debug)]
enum Corruption {
    MissingPointer,
    UnknownPointer,
    ForeignReceipt,
    StaleReceipt,
    CyclicInputPointer,
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
async fn corrupted_last_writer_never_borrows_another_proof(backend: provider_spy::Backend) {
    for fault in [
        Corruption::MissingPointer,
        Corruption::UnknownPointer,
        Corruption::ForeignReceipt,
        Corruption::StaleReceipt,
        Corruption::CyclicInputPointer,
    ] {
        let (storage, spy) = Box::pin(lifecycle_storage(backend)).await;
        let accepted = Box::pin(accept_contact_revision(
            &storage,
            input_source(),
            "independent-contact-root",
            None,
            false,
        ))
        .await;
        let original = Box::pin(finish_accepted_contact(&storage, &accepted)).await;
        let rel = Box::pin(source_contact_rel(&storage, &accepted.source.base.id)).await;
        let canonical = CanonicalRepository::new(storage.clone());
        assert!(
            Box::pin(
                canonical.accepted_contact_rel_support(SPACE, &rel, &accepted.source.base.id,)
            )
            .await
            .unwrap(),
            "positive publication precondition for {fault:?}"
        );
        assert!(!Box::pin(canonical.accepted_contact_rel_support(
            "other-independent-space",
            &rel,
            &accepted.source.base.id,
        ))
        .await
        .unwrap());

        let mut foreign = input_source();
        foreign.external_id = Some("independent-foreign-contact".into());
        foreign.metadata["contact"] = serde_json::json!({
            "name":"Foreign Fern", "email":"fern@example.invalid", "company":"Foreign Company"
        });
        let foreign = Box::pin(accept_contact_revision(
            &storage,
            foreign,
            "independent-foreign-revision",
            None,
            false,
        ))
        .await;
        let foreign_receipt = Box::pin(finish_accepted_contact(&storage, &foreign)).await;
        assert_ne!(accepted.source.base.id, foreign.source.base.id);

        // An actual independent status edit must retain factual proof through
        // its own private last-writer receipt before any corruption is injected.
        let mutation = Box::pin(PublicMutation::begin(storage.clone(), SPACE))
            .await
            .unwrap();
        let mut proposed = rel.clone();
        proposed.status = ClaimStatus::Disputed;
        let mut changed = Box::pin(mutation.replace(&rel, proposed, false, ACTOR))
            .await
            .unwrap();
        assert!(
            Box::pin(canonical.accepted_contact_rel_support(
                SPACE,
                &changed,
                &accepted.source.base.id,
            ))
            .await
            .unwrap(),
            "independent edit positive precondition for {fault:?}"
        );
        let publications = SourcePublicationRepository::new(storage.clone());
        let mut cycle_receipt = None;
        if matches!(fault, Corruption::CyclicInputPointer) {
            cycle_receipt = publications
                .support_for_record(SPACE, &changed.base.id)
                .await
                .unwrap()
                .into_iter()
                .find(|row| row.source_id == accepted.source.base.id)
                .unwrap()
                .last_commit_id;
            // Two real status transitions create compatible Active->Disputed
            // and Disputed->Active edges. Corrupting the first input pointer
            // to the second receipt exercises an actual traversable cycle.
            let mutation = Box::pin(PublicMutation::begin(storage.clone(), SPACE))
                .await
                .unwrap();
            let mut proposed = changed.clone();
            proposed.status = rel.status;
            changed = Box::pin(mutation.replace(&changed, proposed, false, ACTOR))
                .await
                .unwrap();
            assert!(Box::pin(canonical.accepted_contact_rel_support(
                SPACE,
                &changed,
                &accepted.source.base.id,
            ))
            .await
            .unwrap());
        }
        let supports = publications
            .support_for_record(SPACE, &changed.base.id)
            .await
            .unwrap();
        let support = supports
            .iter()
            .find(|row| row.source_id == accepted.source.base.id)
            .unwrap();
        let latest = support.last_commit_id.as_ref().unwrap();
        assert_ne!(latest, &original.commit_id);
        assert_ne!(latest, &foreign_receipt.commit_id);
        let support_key = control_key(&[
            SPACE,
            &support.source_id.to_string(),
            &support.source_revision,
            &support.record_id.to_string(),
        ]);
        let (table, key) = if matches!(fault, Corruption::CyclicInputPointer) {
            let earlier = cycle_receipt.as_ref().unwrap();
            assert_ne!(earlier, latest);
            ("source_commit_receipt", control_key(&[SPACE, earlier]))
        } else {
            ("source_effect_support", support_key)
        };
        let payload = control_payload(&storage, table, &key).await;
        let mut edited: serde_json::Value = serde_json::from_str(&payload).unwrap();
        if matches!(fault, Corruption::CyclicInputPointer) {
            let inputs = edited["_contact_lineage"]["input_support"]
                .as_array_mut()
                .unwrap();
            let input = inputs
                .iter_mut()
                .find(|row| {
                    row["source_id"] == accepted.source.base.id.to_string()
                        && row["record_id"] == changed.base.id.to_string()
                })
                .unwrap();
            assert_ne!(input["last_commit_id"], serde_json::json!(latest));
            input["last_commit_id"] = serde_json::json!(latest);
        } else {
            let decoded: SourceEffectSupport = serde_json::from_value(edited.clone()).unwrap();
            assert_eq!(
                serde_json::to_value(decoded).unwrap(),
                serde_json::to_value(support).unwrap()
            );
            edited["last_commit_id"] = match fault {
                Corruption::MissingPointer => serde_json::Value::Null,
                Corruption::UnknownPointer => {
                    serde_json::json!(format!("absent-{}", uuid::Uuid::new_v4()))
                }
                Corruption::ForeignReceipt => serde_json::json!(foreign_receipt.commit_id),
                Corruption::StaleReceipt => serde_json::json!(original.commit_id),
                Corruption::CyclicInputPointer => unreachable!(),
            };
        }
        let before = Box::pin(graph_rows(&storage)).await;
        let attempts_before = spy.requests().unwrap().len();
        corrupt_control_payload(&storage, table, &key, &payload, &edited.to_string()).await;
        let proof = Box::pin(canonical.accepted_contact_rel_support(
            SPACE,
            &changed,
            &accepted.source.base.id,
        ))
        .await;
        assert!(
            !matches!(proof, Ok(true)),
            "corruption borrowed contact authority: {fault:?}"
        );
        assert_eq!(Box::pin(graph_rows(&storage)).await, before);
        assert_eq!(spy.requests().unwrap().len(), attempts_before);
        // Restore only the exact owned corruption and prove that this was a
        // last-writer boundary failure, not a permanently unusable fixture.
        corrupt_control_payload(&storage, table, &key, &edited.to_string(), &payload).await;
        assert!(Box::pin(canonical.accepted_contact_rel_support(
            SPACE,
            &changed,
            &accepted.source.base.id,
        ))
        .await
        .unwrap());
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
async fn same_revision_plain_acceptance_never_inherits_old_contact_seal(
    backend: provider_spy::Backend,
) {
    let (storage, _spy) = Box::pin(lifecycle_storage(backend)).await;
    let revision = "same-immutable-contact-r1";
    let accepted = Box::pin(accept_contact_revision(
        &storage,
        input_source(),
        revision,
        None,
        false,
    ))
    .await;
    Box::pin(finish_accepted_contact(&storage, &accepted)).await;
    let rel = Box::pin(source_contact_rel(&storage, &accepted.source.base.id)).await;
    let publications = SourcePublicationRepository::new(storage.clone());
    let canonical = CanonicalRepository::new(storage.clone());
    assert!(Box::pin(canonical.accepted_contact_rel_support(
        SPACE,
        &rel,
        &accepted.source.base.id,
    ))
    .await
    .unwrap());
    let immutable = publications
        .revision(SPACE, &accepted.source.base.id, revision)
        .await
        .unwrap()
        .unwrap();
    let before = serde_json::to_value(&immutable).unwrap();
    let prepared = Box::pin(PreparedSourceIntake::prepare(
        &storage,
        immutable.source.clone(),
        IntakeControl {
            revision_id: Some(revision.into()),
            idempotency_key: Some("independent-new-explicit-plain-intent".into()),
            expected_generation: Some(accepted.publication.generation.clone()),
        },
    ))
    .await
    .unwrap();
    let plain = Box::pin(prepared.accept(&storage)).await.unwrap();
    assert_ne!(
        plain.publication.generation,
        accepted.publication.generation
    );
    assert_ne!(
        plain.publication.operation_id,
        accepted.publication.operation_id
    );
    assert_eq!(
        plain.publication.revision_id,
        accepted.publication.revision_id
    );
    assert!(publications
        .accepted_contact_for_publication(SPACE, &plain.publication)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        serde_json::to_value(
            publications
                .revision(SPACE, &accepted.source.base.id, revision)
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        before,
        "fresh plain intent rewrote original immutable input"
    );
    // Pending desired work must not hide the actually published older proof.
    assert!(Box::pin(canonical.accepted_contact_rel_support(
        SPACE,
        &rel,
        &accepted.source.base.id,
    ))
    .await
    .unwrap());
    let receipt = Box::pin(finish_accepted_contact(&storage, &plain)).await;
    assert_eq!(
        receipt.publication_authority.as_ref(),
        Some(&plain.publication)
    );
    assert!(publications
        .accepted_contact_for_publication(SPACE, &plain.publication)
        .await
        .unwrap()
        .is_none());
    assert!(
        !matches!(
            Box::pin(
                canonical.accepted_contact_rel_support(SPACE, &rel, &accepted.source.base.id,)
            )
            .await,
            Ok(true)
        ),
        "same revision label borrowed a prior operation's sealed contact intent"
    );
}

fn independent_contact_source(key: &str) -> Source {
    let mut source = input_source();
    source.external_id = Some(key.into());
    let mut template = source.manual_doc_template().unwrap().unwrap();
    template.rendition_key = format!("qualification/contact/{key}");
    source.set_manual_doc_template(&template).unwrap();
    source
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
async fn shared_contact_pair(
    storage: &StorageClient,
) -> (
    elephant_storage::source_intake::AcceptedSourceIntake,
    elephant_storage::source_intake::AcceptedSourceIntake,
    Rel,
) {
    let first = Box::pin(accept_contact_revision(
        storage,
        independent_contact_source("independent-survival-a"),
        "independent-survival-a-r1",
        None,
        false,
    ))
    .await;
    Box::pin(finish_accepted_contact(storage, &first)).await;
    let second = Box::pin(accept_contact_revision(
        storage,
        independent_contact_source("independent-survival-b"),
        "independent-survival-b-r1",
        None,
        false,
    ))
    .await;
    Box::pin(finish_accepted_contact(storage, &second)).await;
    let shared = Box::pin(source_contact_rel(storage, &second.source.base.id)).await;
    assert!(shared
        .base
        .provenance
        .sources
        .contains(&first.source.base.id));
    assert!(shared
        .base
        .provenance
        .sources
        .contains(&second.source.base.id));
    let canonical = CanonicalRepository::new(storage.clone());
    for accepted in [&first, &second] {
        assert!(Box::pin(canonical.accepted_contact_rel_support(
            SPACE,
            &shared,
            &accepted.source.base.id,
        ))
        .await
        .unwrap());
    }
    (first, second, shared)
}

#[expect(
    clippy::items_after_statements,
    clippy::significant_drop_tightening,
    reason = "Keep the local receipt payload proxy beside its fault injection and preserve repository lifetimes"
)]
async fn erased_a_cannot_revive_completed_plain_b_contact_authority(
    backend: provider_spy::Backend,
) {
    let (storage, spy) = Box::pin(lifecycle_storage(backend)).await;
    let (first, second, shared) = Box::pin(shared_contact_pair(&storage)).await;
    let publications = SourcePublicationRepository::new(storage.clone());
    let canonical = CanonicalRepository::new(storage.clone());
    let immutable = publications
        .revision(
            SPACE,
            &second.source.base.id,
            &second.publication.revision_id,
        )
        .await
        .unwrap()
        .unwrap();
    let prepared = Box::pin(PreparedSourceIntake::prepare(
        &storage,
        immutable.source.clone(),
        IntakeControl {
            revision_id: Some(second.publication.revision_id.clone()),
            idempotency_key: Some("independent-survivor-b-new-plain-operation".into()),
            expected_generation: Some(second.publication.generation.clone()),
        },
    ))
    .await
    .unwrap();
    let plain = Box::pin(prepared.accept(&storage)).await.unwrap();
    assert_eq!(plain.publication.source_id, second.publication.source_id);
    assert_eq!(
        plain.publication.revision_id,
        second.publication.revision_id
    );
    assert_ne!(
        plain.publication.operation_id,
        second.publication.operation_id
    );
    assert_ne!(plain.publication.generation, second.publication.generation);
    assert!(publications
        .accepted_contact_capability_for_publication(SPACE, &plain.publication)
        .await
        .unwrap()
        .is_none());
    // Pending desired generation does not invalidate B's still-published seal.
    assert!(Box::pin(canonical.accepted_contact_rel_support(
        SPACE,
        &shared,
        &second.source.base.id,
    ))
    .await
    .unwrap());
    let completed = Box::pin(finish_accepted_contact(&storage, &plain)).await;
    assert_eq!(
        completed.publication_authority.as_ref(),
        Some(&plain.publication)
    );
    let before_erase = Box::pin(source_contact_rel(&storage, &first.source.base.id)).await;
    assert_eq!(before_erase.base.id, shared.base.id);
    let original_a_supported = Box::pin(canonical.accepted_contact_rel_support(
        SPACE,
        &before_erase,
        &first.source.base.id,
    ))
    .await
    .unwrap();
    if !original_a_supported && storage.embedded_store().is_none() {
        // Diagnostic only: every row comes from this actual synthetic workflow.
        // Never repair a ledger/root or replace the original expected result.
        eprintln!(
            "native contact survival: A={:?}, B={:?}, plain={:?}, plain_commit={}, rel={}, fingerprint={:?}",
            first.publication, second.publication, plain.publication,
            completed.commit_id, before_erase.base.id,
            elephant_storage::canonical_commit::source_support_fingerprint(&before_erase)
        );
        let support = publications
            .support_for_record(SPACE, &before_erase.base.id)
            .await;
        match support.and_then(|rows| {
            serde_json::to_string(&rows).map_err(|error| {
                elephant_storage::StorageError::DataIntegrityError(error.to_string())
            })
        }) {
            Ok(raw) if raw.len() <= 65_536 => eprintln!("actual contact support ledger: {raw}"),
            Ok(raw) => eprintln!(
                "actual contact support diagnostic exceeds 65536 bytes: {}",
                raw.len()
            ),
            Err(error) => eprintln!("actual contact support diagnostic read failed: {error}"),
        }
        let actual_head = Box::pin(publications.head(SPACE, &first.source.base.id)).await;
        let actual_source =
            Box::pin(SourceRepository::new(storage.clone()).get(SPACE, &first.source.base.id))
                .await;
        let preconditions = serde_json::to_string(&serde_json::json!({
            "current_rel": before_erase,
            "a_head": actual_head.map_err(|error| error.to_string()),
            "a_live_source_base": actual_source.map(|source| source.map(|source| source.base)).map_err(|error| error.to_string()),
        }));
        let mut projection_remaining = 65_536usize;
        match preconditions {
            Ok(raw) if raw.len() <= projection_remaining => {
                projection_remaining -= raw.len();
                eprintln!("actual contact proof preconditions: {raw}");
            }
            Ok(raw) => eprintln!(
                "actual contact preconditions exceed diagnostic budget: {} bytes",
                raw.len()
            ),
            Err(error) => eprintln!("actual contact preconditions encode failed: {error}"),
        }
        #[derive(serde::Deserialize)]
        struct ReceiptPayload {
            payload: String,
        }
        match storage
            .query_bind_checked(
                "SELECT payload FROM source_commit_receipt WHERE space_id=$space LIMIT 33;",
                std::collections::BTreeMap::from([("space".to_owned(), SPACE.to_owned())]),
            )
            .await
        {
            Ok(mut response) => match response.take::<Vec<ReceiptPayload>>(0) {
                Ok(rows) if rows.len() <= 32 => {
                    // Project original stored bytes; never synthesize a root or
                    // infer authority from a current support row.
                    fn record_projection(raw: &serde_json::Value) -> serde_json::Value {
                        match serde_json::from_value::<
                            elephant_storage::canonical_commit::CanonicalRecord,
                        >(raw.clone())
                        {
                            Ok(record) => serde_json::json!({
                                "id": record.base().id,
                                "fingerprint": record.fingerprint().map_err(|error| error.to_string()),
                            }),
                            Err(error) => serde_json::json!({"decode_error": error.to_string()}),
                        }
                    }
                    let relation_id = before_erase.base.id.to_string();
                    let mut remaining = projection_remaining;
                    for row in rows {
                        let projected = serde_json::from_str::<serde_json::Value>(&row.payload).and_then(|payload| {
                            let lineage = &payload["_contact_lineage"];
                            let effects = lineage["effects"].as_array().into_iter().flatten()
                                .filter(|effect| ["before", "after"].iter().any(|side|
                                    effect[*side]["record"]["id"].as_str() == Some(relation_id.as_str())))
                                .map(|effect| serde_json::json!({
                                    "before": (!effect["before"].is_null()).then(|| record_projection(&effect["before"])),
                                    "after": (!effect["after"].is_null()).then(|| record_projection(&effect["after"])),
                                })).collect::<Vec<_>>();
                            let dependencies = lineage["dependencies"].as_array().into_iter().flatten()
                                .map(record_projection).collect::<Vec<_>>();
                            let input_support = lineage["input_support"].as_array().into_iter().flatten()
                                .filter(|support| support["record_id"].as_str() == Some(relation_id.as_str()))
                                .collect::<Vec<_>>();
                            serde_json::to_string(&serde_json::json!({
                                "commit_id": payload["commit_id"],
                                "publication_authority": payload["publication_authority"],
                                "effect_ids": payload["effect_ids"],
                                "lineage_version": lineage["version"],
                                "roots": lineage["roots"],
                                "selected_rel_effects": effects,
                                "input_support": input_support,
                                "dependencies": dependencies,
                            }))
                        });
                        match projected {
                            Ok(raw) if raw.len() <= remaining => {
                                remaining -= raw.len();
                                eprintln!("actual contact receipt projection: {raw}");
                            }
                            Ok(raw) => eprintln!("actual receipt projection exceeds remaining diagnostic budget: {} bytes", raw.len()),
                            Err(error) => eprintln!("actual receipt projection decode failed: {error}"),
                        }
                    }
                }
                Ok(rows) => eprintln!(
                    "actual receipt diagnostic exceeds 32 rows: {} rows",
                    rows.len()
                ),
                Err(error) => eprintln!("actual receipt diagnostic decode failed: {error}"),
            },
            Err(error) => eprintln!("actual receipt diagnostic read failed: {error}"),
        }
    }
    assert!(original_a_supported);
    assert!(!matches!(
        Box::pin(canonical.accepted_contact_rel_support(
            SPACE,
            &before_erase,
            &second.source.base.id,
        ))
        .await,
        Ok(true)
    ));
    let survivor_head = publications
        .head(SPACE, &second.source.base.id)
        .await
        .unwrap();
    let attempts = spy.requests().unwrap().len();
    let erased = Box::pin(
        publications.erase_live(
            lifecycle_request(
                &storage,
                &first.source.base.id,
                "independent-erase-a-after-plain-b",
            )
            .await,
        ),
    )
    .await
    .unwrap();
    assert_eq!(erased.state, OperationState::Erased);
    assert!(
        RelRepository::new(storage.clone())
            .get(SPACE, &shared.base.id)
            .await
            .unwrap()
            .is_none_or(|row| row.base.deleted_at.is_some()),
        "old B contact admission must not resurrect A's contact relationship"
    );
    assert!(
        EntityRepository::new(storage.clone())
            .get(SPACE, shared.subject.id())
            .await
            .unwrap()
            .is_none_or(|row| row.base.deleted_at.is_some()),
        "plain B metadata must not authorize an independent contact entity floor"
    );
    assert!(publications
        .support_for_record(SPACE, &shared.base.id)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        publications
            .head(SPACE, &second.source.base.id)
            .await
            .unwrap(),
        survivor_head
    );
    assert_eq!(
        serde_json::to_value(
            publications
                .revision(
                    SPACE,
                    &second.source.base.id,
                    &second.publication.revision_id
                )
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(immutable).unwrap(),
        "erasing A must preserve the actual immutable B input"
    );
    assert_eq!(spy.requests().unwrap().len(), attempts);
}

#[tokio::test]
async fn embedded_erase_a_never_revives_same_revision_completed_plain_b_contact() {
    Box::pin(erased_a_cannot_revive_completed_plain_b_contact_authority(
        provider_spy::Backend::ElephantDb,
    ))
    .await;
}

#[tokio::test]
async fn surreal_erase_a_never_revives_same_revision_completed_plain_b_contact() {
    Box::pin(erased_a_cannot_revive_completed_plain_b_contact_authority(
        provider_spy::Backend::SurrealMemory,
    ))
    .await;
}

#[tokio::test]
async fn embedded_contact_last_writer_corruption_fails_closed_after_real_publication() {
    Box::pin(corrupted_last_writer_never_borrows_another_proof(
        provider_spy::Backend::ElephantDb,
    ))
    .await;
}

#[tokio::test]
async fn surreal_contact_last_writer_corruption_fails_closed_after_real_publication() {
    Box::pin(corrupted_last_writer_never_borrows_another_proof(
        provider_spy::Backend::SurrealMemory,
    ))
    .await;
}

#[tokio::test]
async fn embedded_same_revision_plain_intent_cannot_borrow_old_contact_seal() {
    Box::pin(
        same_revision_plain_acceptance_never_inherits_old_contact_seal(
            provider_spy::Backend::ElephantDb,
        ),
    )
    .await;
}

#[tokio::test]
async fn surreal_same_revision_plain_intent_cannot_borrow_old_contact_seal() {
    Box::pin(
        same_revision_plain_acceptance_never_inherits_old_contact_seal(
            provider_spy::Backend::SurrealMemory,
        ),
    )
    .await;
}

#[test]
fn independent_contact_oracle_future_types_fit_the_fixture_stack_budget() {
    fn size<F: std::future::Future>(_: impl FnOnce(provider_spy::Backend) -> F) -> usize {
        std::mem::size_of::<F>()
    }
    for bytes in [
        size(corrupted_last_writer_never_borrows_another_proof),
        size(same_revision_plain_acceptance_never_inherits_old_contact_seal),
        size(erased_a_cannot_revive_completed_plain_b_contact_authority),
    ] {
        assert!(
            bytes <= 64 * 1024,
            "independent contact fixture future is {bytes} bytes"
        );
    }
}
