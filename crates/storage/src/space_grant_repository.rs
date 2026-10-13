//! Current service grants with generation-checked administrative replacement.

use chrono::Utc;
use elephant_types::records::SpaceConfig;
use elephant_types::space_grant::SpaceGrant;
use serde::Deserialize;
use surrealdb::sql::{Thing, Value};
use uuid::Uuid;

use crate::{StorageClient, StorageError, StorageResult};

const TABLE: &str = "space_grant";

#[derive(Deserialize)]
struct GrantDb {
    payload: String,
}

/// The local service-grant authority, consulted on every multi-space dispatch.
#[derive(Clone, Debug)]
pub struct SpaceGrantRepository {
    client: StorageClient,
}

impl SpaceGrantRepository {
    /// Creates a repository using the caller's active storage.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Reads the current grant, including revocations.
    pub async fn get(&self, space: &str, subject: &str) -> StorageResult<Option<SpaceGrant>> {
        let key = grant_key(space, subject);
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    let row = tx
                        .scan_extension_rows(TABLE)?
                        .into_iter()
                        .find(|(id, _)| id == &format!("{TABLE}:{key}"));
                    row.map(|(_, row)| {
                        let payload = row
                            .get("payload")
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(|| integrity("missing grant payload"))?;
                        decode(payload, space, subject)
                    })
                    .transpose()
                })
                .await;
        }
        let mut response = self.client.query_bind_checked(
            "SELECT payload FROM $id WHERE space_id = $space AND subject = $subject",
            crate::bindings! { "id" => Thing::from((TABLE, key.as_str())), "space" => space, "subject" => subject },
        ).await?;
        let rows: Vec<GrantDb> = response
            .take(0)
            .map_err(|error| integrity(&error.to_string()))?;
        rows.into_iter()
            .next()
            .map(|row| decode(&row.payload, space, subject))
            .transpose()
    }

    /// Creates or replaces a grant only at the observed administrative generation.
    /// `None` creates an absent grant; revocation retains the row and rotates it.
    pub async fn replace(
        &self,
        mut grant: SpaceGrant,
        expected: Option<&str>,
    ) -> StorageResult<SpaceGrant> {
        if [
            grant.space_id.as_str(),
            grant.subject.as_str(),
            grant.updated_by.as_str(),
        ]
        .iter()
        .any(|value| value.trim().is_empty() || value.chars().any(char::is_control))
            || grant.scopes.len() > 128
            || grant
                .scopes
                .iter()
                .any(|scope| scope.is_empty() || scope.len() > 128)
        {
            return Err(integrity("invalid grant identity or scopes"));
        }
        let old = self.get(&grant.space_id, &grant.subject).await?;
        if old.as_ref().map(|old| old.generation.as_str()) != expected {
            return Err(StorageError::Conflict("grant generation changed".into()));
        }
        grant.generation = Uuid::new_v4().to_string();
        grant.updated_at = Utc::now();
        let key = grant_key(&grant.space_id, &grant.subject);
        let payload =
            serde_json::to_string(&grant).map_err(|error| integrity(&error.to_string()))?;
        let old_payload = old
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| integrity(&error.to_string()))?;
        if let Some(store) = self.client.embedded_store() {
            store
                .mutate(|tx| {
                    if !tx.scan::<SpaceConfig>()?.iter().any(|config| {
                        config.base.space_id == grant.space_id && config.base.deleted_at.is_none()
                    }) {
                        return Err(StorageError::NotFound("space".into()));
                    }
                    let current = tx
                        .scan_extension_rows(TABLE)?
                        .into_iter()
                        .find(|(id, _)| id == &format!("{TABLE}:{key}"));
                    let current_payload = current
                        .as_ref()
                        .and_then(|(_, row)| row.get("payload"))
                        .and_then(serde_json::Value::as_str);
                    if current_payload != old_payload.as_deref() {
                        return Err(StorageError::Conflict("grant generation changed".into()));
                    }
                    tx.upsert_extension_row(
                        TABLE,
                        serde_json::json!({
                            "id":format!("{TABLE}:{key}"), "space_id":grant.space_id,
                            "subject":grant.subject,"payload":payload,
                        }),
                    )?;
                    tx.advance_canonical_epoch(&grant.space_id)?;
                    Ok(())
                })
                .await?;
        } else {
            let record = crate::surreal::json_to_surreal(serde_json::json!({
                "space_id":grant.space_id,"subject":grant.subject,"payload":payload,
            }))?;
            self.client.query_bind_checked(
                "BEGIN TRANSACTION; LET $space_rows = SELECT id FROM space_config WHERE space_id = $space AND deleted_at IS NONE; IF array::len($space_rows) != 1 { THROW 'grant:unknown_space'; }; LET $current = SELECT VALUE payload FROM $id; IF array::first($current) != $expected { THROW 'grant:conflict_generation'; }; UPSERT $id CONTENT $record; COMMIT TRANSACTION;",
                crate::bindings! {
                    "id" => Thing::from((TABLE,key.as_str())), "space" => grant.space_id.as_str(),
                    "expected" => old_payload.map_or(Value::None, Value::from), "record" => record,
                },
            ).await.map_err(|error| {
                if error.to_string().contains("grant:conflict_generation") { StorageError::Conflict("grant generation changed".into()) }
                else if error.to_string().contains("grant:unknown_space") { StorageError::NotFound("space".into()) }
                else { error }
            })?;
        }
        Ok(grant)
    }
}

pub(crate) fn grant_key(space: &str, subject: &str) -> String {
    Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!("{}:{space}{}:{subject}", space.len(), subject.len()).as_bytes(),
    )
    .to_string()
}

fn decode(payload: &str, space: &str, subject: &str) -> StorageResult<SpaceGrant> {
    let grant: SpaceGrant =
        serde_json::from_str(payload).map_err(|error| integrity(&error.to_string()))?;
    if grant.space_id != space || grant.subject != subject || grant.generation.is_empty() {
        return Err(integrity("grant identity mismatch"));
    }
    Ok(grant)
}

fn integrity(message: &str) -> StorageError {
    StorageError::DataIntegrityError(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Repository, SpaceConfigRepository, StorageConfig};
    use elephant_types::{RecordId, TableName};
    use std::collections::BTreeSet;

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn grant_revocation_rotates_generation_and_prevents_stale_regrant(
    ) -> Result<(), Box<dyn std::error::Error>> {
        for embedded in [true, false] {
            let client = if embedded {
                StorageClient::new_embedded().await?
            } else {
                StorageClient::new(StorageConfig::default()).await?
            };
            if !embedded {
                client
                    .query_checked(include_str!("../../migrations/sql/036_space_grants.surql"))
                    .await?;
            }
            let config: SpaceConfig = serde_json::from_value(serde_json::json!({
                "id":RecordId::new_v4(TableName::SpaceConfig),"space_id":"alpha",
                "created_at":Utc::now(),"updated_at":Utc::now(),"created_by":"operator",
                "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
                "provenance":{},"subjects":[],"subjects_state":"complete",
                "embedding_provider":"fake","embedding_model":"fixture","embedding_dims":3,
                "purpose":null,"skill":null
            }))?;
            SpaceConfigRepository::new(client.clone())
                .create(config)
                .await?;
            let repo = SpaceGrantRepository::new(client);
            let grant = SpaceGrant {
                space_id: "alpha".into(),
                subject: "service".into(),
                generation: String::new(),
                active: true,
                scopes: BTreeSet::from(["read:source".into()]),
                updated_by: "operator".into(),
                updated_at: Utc::now(),
            };
            let first = repo.replace(grant.clone(), None).await?;
            assert!(repo.get("beta", "service").await?.is_none());
            let mut revoked = first.clone();
            revoked.active = false;
            let current = repo.replace(revoked, Some(&first.generation)).await?;
            assert_ne!(current.generation, first.generation);
            assert!(matches!(
                repo.replace(grant, Some(&first.generation)).await,
                Err(StorageError::Conflict(_))
            ));
            assert!(
                !repo
                    .get("alpha", "service")
                    .await?
                    .ok_or("missing grant")?
                    .active
            );
        }
        Ok(())
    }
}
