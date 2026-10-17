//! Indexed candidates are checked against their complete typed support payload.
use super::*;
use crate::canonical_commit::{support_key, SourceEffectSupport};
use crate::historical_adoption::HistoricalUnknownOwnership;

const MAX_SUPPORTS_PER_RECORD: usize = 4096;

#[derive(Deserialize)]
struct SupportRow {
    physical_id: String,
    space_id: String,
    source_id: String,
    source_revision: String,
    record_id: String,
    independent: bool,
    #[serde(default)]
    record_fingerprint: Option<String>,
    #[serde(default)]
    historical_unknown: Option<HistoricalUnknownOwnership>,
    #[serde(default)]
    state: Option<SourceEffectSupport>,
    payload: String,
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

impl SupportRow {
    fn validate(self, space: &str, record: &RecordId) -> StorageResult<SourceEffectSupport> {
        let support: SourceEffectSupport = decode_payload(&self.payload)?;
        let key = support_key(space, &support.source_id, &support.source_revision, record);
        if (self.physical_id != format!("source_effect_support:{key}")
            && self.physical_id != control_thing("source_effect_support", &key).to_string())
            || self.space_id != space
            || support.space_id != space
            || self.record_id != record.to_string()
            || support.record_id != *record
            || self.source_id != support.source_id.to_string()
            || self.source_revision != support.source_revision
            || self.independent != support.independent
            || self.historical_unknown != support.historical_unknown
            || self.state.is_some_and(|state| state != support)
            || self
                .record_fingerprint
                .as_ref()
                .is_some_and(|fp| fp != &support.record_fingerprint)
            || support.source_id.table() != TableName::Source
            || support.source_revision.is_empty()
            || !Uuid::parse_str(&support.record_fingerprint).is_ok_and(|fp| {
                fp.get_version_num() == 5
                    && fp.get_variant() == uuid::Variant::RFC4122
                    && fp.to_string() == support.record_fingerprint
            })
            || support.historical_unknown.as_ref().is_some_and(|history| {
                support.independent
                    || support.last_commit_id.as_deref() != Some(history.commit_id.as_str())
                    || !valid_digest(&history.snapshot_sha256)
                    || !valid_digest(&history.plan_id)
            })
        {
            return Err(integrity("source support identity or projection differs"));
        }
        Ok(support)
    }
}

impl SourcePublicationRepository {
    async fn support_nonce(&self) -> StorageResult<String> {
        #[derive(Deserialize)]
        struct Nonce {
            support_nonce: String,
        }
        let mut result = self.client.query_checked(
            "SELECT IF fn::recall_support_index_fence_valid($this) THEN support_nonce ELSE NONE END AS support_nonce FROM ONLY canonical_epoch:['recall_source_support_index',1];"
        ).await?;
        let row: Option<Nonce> = result
            .take(0)
            .map_err(|_| integrity("support index fence invalid"))?;
        let row = row.ok_or_else(|| integrity("support index fence migration required"))?;
        if Uuid::parse_str(&row.support_nonce).is_err() {
            return Err(integrity("support index fence invalid"));
        }
        Ok(row.support_nonce)
    }

    async fn validated_support_nonce(&self) -> StorageResult<String> {
        let mut cached = self.client.support_validation.lock().await;
        let nonce = self.support_nonce().await?;
        if cached.as_ref() == Some(&nonce) {
            return Ok(nonce);
        }
        // One bounded paginated validation per mutation nonce, not per node.
        // Query by physical key without the untrusted projections as filters.
        let mut after = Value::None;
        let mut total = 0usize;
        loop {
            let query = if matches!(after, Value::None) {
                "SELECT *,type::string(id) AS physical_id FROM source_effect_support ORDER BY id LIMIT 256;"
            } else {
                "SELECT *,type::string(id) AS physical_id FROM source_effect_support WHERE id>$after ORDER BY id LIMIT 256;"
            };
            let mut result = self
                .client
                .query_bind_checked(query, crate::bindings! {"after"=>after.clone()})
                .await?;
            let rows: Vec<SupportRow> = result
                .take(0)
                .map_err(|_| integrity("source support row invalid"))?;
            let count = rows.len();
            for row in rows {
                let physical: Thing = row
                    .physical_id
                    .parse()
                    .map_err(|()| integrity("source support physical identity invalid"))?;
                let payload: SourceEffectSupport = decode_payload(&row.payload)?;
                row.validate(&payload.space_id, &payload.record_id)?;
                after = physical.into();
                total += 1;
                if total > 4_000_000 {
                    return Err(integrity("source support corpus validation bound"));
                }
            }
            if count < 256 {
                break;
            }
        }
        if self.support_nonce().await? != nonce {
            return Err(conflict("source support changed during validation"));
        }
        *cached = Some(nonce.clone());
        drop(cached);
        Ok(nonce)
    }

    /// Exact record candidates, bounded before payload allocation and validated
    /// against physical identity and all authority projections. This method
    /// does not replace current head/receipt/evidence checks in read eligibility.
    pub async fn support_for_record(
        &self,
        space: &str,
        record: &RecordId,
    ) -> StorageResult<Vec<SourceEffectSupport>> {
        let rows: Vec<SupportRow> = if let Some(store) = self.client.embedded_store() {
            store
                .source_support_rows(space, &record.to_string(), MAX_SUPPORTS_PER_RECORD)
                .await?
                .into_iter()
                .map(|(key, mut row)| {
                    if row.get("id").and_then(serde_json::Value::as_str) != Some(key.as_str()) {
                        return Err(integrity("source support physical identity differs"));
                    }
                    row["physical_id"] = key.into();
                    serde_json::from_value(row).map_err(|_| integrity("source support row invalid"))
                })
                .collect::<StorageResult<_>>()?
        } else {
            let nonce = self.validated_support_nonce().await?;
            let mut result = self.client.query_bind_checked(
                "SELECT *, type::string(id) AS physical_id FROM source_effect_support WITH INDEX source_support_space_record WHERE space_id=$space AND record_id=$record LIMIT 4097;",
                crate::bindings! {"space"=>space,"record"=>record.to_string()},
            ).await?;
            let rows = result
                .take(0)
                .map_err(|_| integrity("source support row invalid"))?;
            if self.support_nonce().await? != nonce {
                return Err(conflict("source support changed during lookup"));
            }
            rows
        };
        if rows.len() > MAX_SUPPORTS_PER_RECORD {
            return Err(integrity("source support record exceeds lookup bound"));
        }
        rows.into_iter()
            .map(|row| row.validate(space, record))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;
    use serde_json::json;

    fn support() -> SourceEffectSupport {
        SourceEffectSupport {
            space_id: "s".into(),
            source_id: "source:00000000-0000-4000-8000-000000000001"
                .parse()
                .unwrap(),
            source_revision: "r1".into(),
            record_id: "doc_node:00000000-0000-4000-8000-000000000001"
                .parse()
                .unwrap(),
            historical_unknown: None,
            independent: false,
            record_fingerprint: stable_key(&["original row"]),
            last_commit_id: Some("commit".into()),
        }
    }
    fn row(support: &SourceEffectSupport) -> serde_json::Value {
        json!({"physical_id":format!("source_effect_support:{}",support_key(&support.space_id,&support.source_id,&support.source_revision,&support.record_id)),"space_id":support.space_id,"source_id":support.source_id.to_string(),"source_revision":support.source_revision,"record_id":support.record_id.to_string(),"independent":support.independent,"payload":encode_payload(support).unwrap()})
    }
    #[test]
    fn support_candidates_preserve_normal_and_historical_authority_and_refuse_mismatch() {
        let mut support = support();
        for historical in [false, true] {
            if historical {
                support.historical_unknown = Some(HistoricalUnknownOwnership {
                    snapshot_sha256: "a".repeat(64),
                    plan_id: "b".repeat(64),
                    commit_id: "commit".into(),
                });
            }
            let mut original = row(&support);
            original["historical_unknown"] =
                serde_json::to_value(&support.historical_unknown).unwrap();
            assert_eq!(
                serde_json::from_value::<SupportRow>(original.clone())
                    .unwrap()
                    .validate("s", &support.record_id)
                    .unwrap(),
                support
            );
            for field in [
                "physical_id",
                "source_id",
                "source_revision",
                "record_id",
                "space_id",
            ] {
                let mut corrupted = original.clone();
                corrupted[field] = "wrong".into();
                assert!(
                    serde_json::from_value::<SupportRow>(corrupted)
                        .unwrap()
                        .validate("s", &support.record_id)
                        .is_err(),
                    "{field}"
                );
            }
            let mut corrupted = original.clone();
            corrupted["independent"] = true.into();
            assert!(serde_json::from_value::<SupportRow>(corrupted)
                .unwrap()
                .validate("s", &support.record_id)
                .is_err());
            if historical {
                let mut corrupted = original;
                corrupted
                    .as_object_mut()
                    .unwrap()
                    .remove("historical_unknown");
                assert!(serde_json::from_value::<SupportRow>(corrupted)
                    .unwrap()
                    .validate("s", &support.record_id)
                    .is_err());
            }
        }
    }
    async fn put(client: &StorageClient, support: &SourceEffectSupport, bad_projection: bool) {
        let key = support_key(
            &support.space_id,
            &support.source_id,
            &support.source_revision,
            &support.record_id,
        );
        let mut value = row(support);
        value.as_object_mut().unwrap().remove("physical_id");
        if bad_projection {
            value["source_revision"] = "different".into();
        }
        if let Some(store) = client.embedded_store() {
            value["id"] = format!("source_effect_support:{key}").into();
            store
                .mutate(|tx| {
                    tx.upsert_extension_row("source_effect_support", value)?;
                    Ok(())
                })
                .await
                .unwrap();
        } else {
            client
                .query_bind_checked(
                    "UPSERT $id CONTENT $value;",
                    crate::bindings! {
                        "id"=>control_thing("source_effect_support",&key),
                        "value"=>crate::surreal::json_to_surreal(value).unwrap(),
                    },
                )
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original native client and repository custody alive through every corruption/refusal/readback assertion and final scenario teardown."
    )]
    async fn support_exact_backend_lookup_rejects_payload_projection_drift() {
        for embedded in [true, false] {
            let client = super::super::tests::client(embedded).await;
            if !embedded {
                client
                    .query_checked(include_str!(
                        "../../migrations/sql/046_source_support_index_fence.surql"
                    ))
                    .await
                    .unwrap();
            }
            let repository = SourcePublicationRepository::new(client.clone());
            let mut wanted = support();
            wanted.space_id = "business-a".into();
            put(&client, &wanted, false).await;
            let mut foreign = wanted.clone();
            foreign.space_id = "foreign".into();
            put(&client, &foreign, false).await;
            let mut unrelated = wanted.clone();
            unrelated.record_id = "doc_node:00000000-0000-4000-8000-000000000002"
                .parse()
                .unwrap();
            put(&client, &unrelated, false).await;
            assert_eq!(
                repository
                    .support_for_record("business-a", &wanted.record_id)
                    .await
                    .unwrap(),
                vec![wanted.clone()]
            );
            if !embedded {
                let key = support_key(
                    &wanted.space_id,
                    &wanted.source_id,
                    &wanted.source_revision,
                    &wanted.record_id,
                );
                client.query_bind_checked("UPDATE $id SET record_id=$wrong;",crate::bindings!{
                    "id"=>control_thing("source_effect_support",&key),"wrong"=>unrelated.record_id.to_string()
                }).await.unwrap();
                // Previously cached wanted candidate must not vanish into the
                // empty-support fallback after moving only the projection.
                assert!(repository
                    .support_for_record("business-a", &wanted.record_id)
                    .await
                    .is_err());
                put(&client, &wanted, false).await;
            }
            put(&client, &wanted, true).await;
            assert!(repository
                .support_for_record("business-a", &wanted.record_id)
                .await
                .is_err());
            put(&client, &wanted, false).await;
            assert_eq!(
                repository
                    .support_for_record("business-a", &wanted.record_id)
                    .await
                    .unwrap(),
                vec![wanted]
            );
        }
    }
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original native client and repository custody alive through every corruption/refusal/readback assertion and final scenario teardown."
    )]
    async fn global_support_fence_cannot_collide_with_same_named_tenant() {
        let client = StorageClient::new_memory().await.unwrap();
        client
            .query_checked(concat!(
                include_str!("../../migrations/sql/031_source_publication.surql"),
                "\n",
                include_str!("../../migrations/sql/046_source_support_index_fence.surql")
            ))
            .await
            .unwrap();
        let repository = SourcePublicationRepository::new(client.clone());
        let before = repository.support_nonce().await.unwrap();
        client
            .query_checked(
                "CREATE source:tenant_fixture SET space_id='recall_source_support_index';",
            )
            .await
            .unwrap();
        let epoch = crate::canonical_commit::CanonicalRepository::new(client.clone())
            .observe_epoch("recall_source_support_index")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(repository.support_nonce().await.unwrap(), before);
        let mut support = support();
        support.space_id = "recall_source_support_index".into();
        put(&client, &support, false).await;
        assert_ne!(repository.support_nonce().await.unwrap(), before);
        assert_ne!(
            crate::canonical_commit::CanonicalRepository::new(client.clone())
                .observe_epoch("recall_source_support_index")
                .await
                .unwrap()
                .as_deref(),
            Some(epoch.as_str())
        );
        assert_eq!(
            repository
                .support_for_record("recall_source_support_index", &support.record_id)
                .await
                .unwrap(),
            vec![support]
        );
        let after_support = repository.support_nonce().await.unwrap();
        client
            .query_checked("UPDATE source:tenant_fixture SET marker=true;")
            .await
            .unwrap();
        assert_eq!(repository.support_nonce().await.unwrap(), after_support);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original native client and repository custody alive through every corruption/refusal/readback assertion and final scenario teardown."
    )]
    async fn restored_epoch_rotation_preserves_disjoint_support_fence_and_rejects_corruption(
    ) -> StorageResult<()> {
        let client = StorageClient::new_memory().await?;
        client
            .query_checked(concat!(
                include_str!("../../migrations/sql/031_source_publication.surql"),
                "\n",
                include_str!("../../migrations/sql/046_source_support_index_fence.surql")
            ))
            .await?;
        let repository = SourcePublicationRepository::new(client.clone());
        let canonical = crate::canonical_commit::CanonicalRepository::new(client.clone());
        // Both valid string tenant names remain disjoint from the array control.
        let spaces = [
            "ordinary",
            "recall_source_support_index",
            "__recall_source_support_index__",
        ];
        let mut original = Vec::new();
        for space in spaces {
            original.push(canonical.epoch(space).await?);
        }
        let nonce = repository.support_nonce().await?;
        repository.rotate_canonical_epochs().await?;
        assert_ne!(repository.support_nonce().await?, nonce);
        let mut rotated = Vec::new();
        for (space, before) in spaces.into_iter().zip(original) {
            let after = canonical.current_epoch(space).await?;
            assert_ne!(after, before);
            rotated.push(after);
        }
        // The exact production shape validator, not a field-only projection,
        // is used by support_nonce above. A fourth field must remain an error.
        client
            .query_checked(
                "UPDATE canonical_epoch:['recall_source_support_index',1] SET epoch='unexpected';",
            )
            .await?;
        assert!(repository.support_nonce().await.is_err());
        let before = support_fence_image(&client).await?;
        assert!(repository.rotate_canonical_epochs().await.is_err());
        assert_eq!(support_fence_image(&client).await?, before);
        for (space, before) in spaces.into_iter().zip(rotated) {
            assert_eq!(canonical.current_epoch(space).await?, before);
        }
        // An absent global support control stays absent while actual tenants rotate;
        // this fixture retains the 046 validator function.
        client
            .query_checked("DELETE canonical_epoch:['recall_source_support_index',1];")
            .await?;
        let before = canonical.current_epoch("ordinary").await?;
        repository.rotate_canonical_epochs().await?;
        assert_ne!(canonical.current_epoch("ordinary").await?, before);
        assert_eq!(support_fence_image(&client).await?, None);
        Ok(())
    }

    async fn support_fence_image(client: &StorageClient) -> StorageResult<Option<String>> {
        let mut response = client.query_checked(
            "LET $row = (SELECT * FROM ONLY canonical_epoch:['recall_source_support_index',1]); RETURN IF $row = NONE THEN NONE ELSE type::string($row) END;"
        ).await?;
        response
            .take::<Option<String>>(1)
            .map_err(|_| integrity("support fence fixture projection invalid"))
    }
}
