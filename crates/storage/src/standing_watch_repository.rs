//! Private durable recurring authority.
//!
//! A serialized descriptor is evidence to check, never a caller-minted capability. Only
//! verified or explicit trusted registration can create it; every later use observes
//! current authority again.
use std::collections::BTreeMap;

use elephant_auth::standing_watch::{
    attenuate_current_grant, StandingWatchAdmission, StandingWatchOrigin,
};
use elephant_policy::Principal;
use elephant_types::publication::PublicationFence;
use elephant_types::records::{SourceWatch, SourceWatchStatus, SpaceConfig, WorkItem, WorkRun};
use elephant_types::space_grant::SpaceGrant;
use elephant_types::{RecordId, TableName};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use surrealdb::sql::{Thing, Value};
use uuid::Uuid;

use crate::{EmbeddedMutation, Repository, StorageClient, StorageError, StorageResult};

pub(crate) const TABLE: &str = "watch_delegation";
/// Immutable queue payload reference. It proves nothing without the private row.
pub const PAYLOAD_KEY: &str = "standing_watch_delegation";

/// The identity bound into the original immutable queue intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StandingWatchIdentity {
    /// Unique explicit registration, rotated only by authorized reconfiguration.
    pub id: String,
    /// Digest of the exact immutable stored descriptor bytes.
    pub digest: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Origin {
    VerifiedService {
        subject: String,
        issuer: Option<String>,
        audience: Option<Vec<String>>,
        registered_token_expires_at: i64,
    },
    TrustedLocal,
}

#[derive(Clone, Serialize, Deserialize)]
struct Descriptor {
    version: u32,
    delegation_id: String,
    space_id: String,
    watch_id: RecordId,
    source_id: RecordId,
    watch_intent_fingerprint: String,
    original_ceiling: Principal,
    origin: Origin,
}

#[derive(Clone)]
struct Stored {
    descriptor: Descriptor,
    payload: String,
    identity: StandingWatchIdentity,
}

#[derive(Deserialize)]
struct RowDb {
    payload: String,
    digest: String,
}

fn refused() -> StorageError {
    StorageError::Conflict("standing_watch_authority_changed_or_missing".into())
}
fn invalid(error: impl std::fmt::Display) -> StorageError {
    StorageError::DataIntegrityError(error.to_string())
}
fn checksum(payload: &str) -> String {
    format!("{:x}", Sha256::digest(payload.as_bytes()))
}
fn row_id(watch: &RecordId) -> Thing {
    Thing::from((TABLE, surrealdb::sql::Id::String(watch.id().to_string())))
}
fn row_key(watch: &RecordId) -> String {
    format!("{TABLE}:{}", watch.id())
}
fn grant_id(grant: &SpaceGrant) -> Thing {
    Thing::from((
        "space_grant",
        surrealdb::sql::Id::String(crate::space_grant_repository::grant_key(
            &grant.space_id,
            &grant.subject,
        )),
    ))
}
fn grant_payload(grant: &SpaceGrant) -> StorageResult<String> {
    serde_json::to_string(grant).map_err(invalid)
}
async fn read_grant(
    client: &StorageClient,
    space: &str,
    subject: &str,
) -> StorageResult<Option<SpaceGrant>> {
    if let Some(store) = client.embedded_store() {
        return crate::private_control_read::payload(
            &store,
            "space_grant",
            space,
            &crate::space_grant_repository::grant_key(space, subject),
        )
        .await?
        .map(|payload| {
            let grant: SpaceGrant = serde_json::from_str(&payload).map_err(invalid)?;
            if grant.space_id != space || grant.subject != subject || grant.generation.is_empty() {
                return Err(refused());
            }
            Ok(grant)
        })
        .transpose();
    }
    crate::space_grant_repository::SpaceGrantRepository::new(client.clone())
        .get(space, subject)
        .await
}
fn semantic_intent(watch: &SourceWatch) -> StorageResult<String> {
    crate::source_watch_producer::watch_intent_fingerprint(watch, &watch.base.created_by)
}
const fn active(watch: &SourceWatch) -> bool {
    watch.base.deleted_at.is_none()
        && matches!(
            watch.status,
            SourceWatchStatus::Enabled | SourceWatchStatus::Error
        )
}

impl Stored {
    fn decode(watch: &SourceWatch, payload: String, digest: String) -> StorageResult<Self> {
        let descriptor: Descriptor = serde_json::from_str(&payload).map_err(invalid)?;
        if descriptor.version != 1
            || Uuid::parse_str(&descriptor.delegation_id).is_err()
            || descriptor.space_id != watch.base.space_id
            || descriptor.watch_id != watch.base.id
            || descriptor.source_id != *watch.source.id()
            || descriptor.original_ceiling.space_id != watch.base.space_id
            || digest != checksum(&payload)
        {
            return Err(invalid("invalid standing watch descriptor"));
        }
        let identity = StandingWatchIdentity {
            id: descriptor.delegation_id.clone(),
            digest,
        };
        Ok(Self {
            descriptor,
            payload,
            identity,
        })
    }

    fn embedded(&self) -> serde_json::Value {
        serde_json::json!({
            "id":row_key(&self.descriptor.watch_id), "space_id":self.descriptor.space_id,
            "watch_id":self.descriptor.watch_id, "source_id":self.descriptor.source_id,
            "payload":self.payload, "digest":self.identity.digest,
        })
    }

    fn native(&self) -> Value {
        Value::Object(surrealdb::sql::Object::from(crate::bindings! {
            "space_id"=>self.descriptor.space_id.clone(),
            "watch_id"=>crate::surreal::thing_from_record_id(&self.descriptor.watch_id),
            "source_id"=>crate::surreal::thing_from_record_id(&self.descriptor.source_id),
            "payload"=>self.payload.clone(), "digest"=>self.identity.digest.clone(),
        }))
    }
}

/// Owner of private delegation rows, independent of scheduler actor strings.
#[derive(Clone, Debug)]
pub struct StandingWatchRepository {
    client: StorageClient,
}

impl StandingWatchRepository {
    /// Use the same active storage as the source/watch transaction owner.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    async fn stored(&self, watch: &SourceWatch) -> StorageResult<Option<Stored>> {
        if let Some(store) = self.client.embedded_store() {
            return crate::private_control_read::row(
                &store,
                TABLE,
                &watch.base.space_id,
                &watch.base.id.id().to_string(),
            )
            .await?
            .map(|value| {
                if value["space_id"] != watch.base.space_id
                    || value["watch_id"] != watch.base.id.to_string()
                    || value["source_id"] != watch.source.id().to_string()
                {
                    return Err(refused());
                }
                Stored::decode(
                    watch,
                    value["payload"].as_str().ok_or_else(refused)?.to_owned(),
                    value["digest"].as_str().ok_or_else(refused)?.to_owned(),
                )
            })
            .transpose();
        }
        let mut response = self.client.query_bind_checked(
            "SELECT payload,digest FROM $standing_watch_id WHERE space_id=$standing_watch_space AND watch_id=$standing_watch_watch AND source_id=$standing_watch_source",
            crate::bindings! { "standing_watch_id"=>row_id(&watch.base.id), "standing_watch_space"=>watch.base.space_id.as_str(),
                "standing_watch_watch"=>crate::surreal::thing_from_record_id(&watch.base.id),
                "standing_watch_source"=>crate::surreal::thing_from_record_id(watch.source.id()), }
        ).await?;
        let rows: Vec<RowDb> = response.take(0).map_err(invalid)?;
        if rows.len() > 1 {
            return Err(invalid("duplicate standing watch descriptor"));
        }
        rows.into_iter()
            .next()
            .map(|row| Stored::decode(watch, row.payload, row.digest))
            .transpose()
    }

    async fn configuration(&self, space: &str) -> StorageResult<SpaceConfig> {
        crate::SpaceConfigRepository::new(self.client.clone())
            .get_by_space_id(space)
            .await?
            .ok_or_else(refused)
    }

    /// Prepare a new explicit delegation.
    ///
    /// I's registration owner authorizes the
    /// complete old/new Watch and affected Source/Docs, then commits this proof
    /// together with its exact record readset, Watch and initial queue intent.
    /// # Errors
    /// Refuses changed admission grants, unknown spaces or mismatched intent.
    pub async fn prepare_registration(
        &self,
        watch: &SourceWatch,
        admission: &StandingWatchAdmission,
    ) -> StorageResult<PreparedStandingWatchRegistration> {
        if watch.base.id.table() != TableName::SourceWatch
            || watch.base.deleted_at.is_some()
            || admission.principal().space_id != watch.base.space_id
        {
            return Err(refused());
        }
        let previous_watch = crate::SourceWatchRepository::new(self.client.clone())
            .get(&watch.base.space_id, &watch.base.id)
            .await?;
        let previous = self.stored(watch).await?;
        let configuration = self.configuration(&watch.base.space_id).await?;
        let grant = admission.registration_grant().cloned();
        let origin = match admission.origin() {
            StandingWatchOrigin::VerifiedService {
                subject,
                issuer,
                audience,
                registered_token_expires_at,
            } => {
                let current = read_grant(&self.client, &watch.base.space_id, subject).await?;
                if current != grant || grant.is_none() {
                    return Err(refused());
                }
                Origin::VerifiedService {
                    subject: subject.clone(),
                    issuer: issuer.clone(),
                    audience: audience.clone(),
                    registered_token_expires_at: *registered_token_expires_at,
                }
            }
            StandingWatchOrigin::TrustedLocal => {
                if grant.is_some() {
                    return Err(refused());
                }
                Origin::TrustedLocal
            }
        };
        let descriptor = Descriptor {
            version: 1,
            delegation_id: Uuid::new_v4().to_string(),
            space_id: watch.base.space_id.clone(),
            watch_id: watch.base.id.clone(),
            source_id: watch.source.id().clone(),
            watch_intent_fingerprint: semantic_intent(watch)?,
            original_ceiling: admission.principal().clone(),
            origin,
        };
        let payload = serde_json::to_string(&descriptor).map_err(invalid)?;
        let next = Stored::decode(watch, payload.clone(), checksum(&payload))?;
        Ok(PreparedStandingWatchRegistration {
            watch: watch.clone(),
            previous_watch,
            previous,
            next,
            configuration,
            grant,
        })
    }

    /// Prepare initial local registration ONLY when durable authority is absent.
    ///
    /// Existing local authority must have the exact selected original ceiling;
    /// existing service authority refuses. The returned absence is fenced by the
    /// same registration transaction, never by this read alone.
    /// # Errors
    /// Refuses signed/different local authority or a raced registration.
    pub async fn prepare_local_registration_if_absent(
        &self,
        watch: &SourceWatch,
        admission: &StandingWatchAdmission,
    ) -> StorageResult<Option<PreparedStandingWatchRegistration>> {
        if !matches!(admission.origin(), StandingWatchOrigin::TrustedLocal) {
            return Err(refused());
        }
        if self.stored(watch).await?.is_some() {
            self.capture_for_local_scheduling(watch, admission).await?;
            return Ok(None);
        }
        let prepared = self.prepare_registration(watch, admission).await?;
        if prepared.previous.is_some() {
            return Err(refused());
        }
        Ok(Some(prepared))
    }

    /// Exercise an existing explicit local registration through a real queue
    /// attempt.
    ///
    /// A signed registration cannot be adopted by a local convenience
    /// caller, and the original local ceiling must remain exactly selected.
    /// # Errors
    /// Refuses missing/signed authority or a different local principal ceiling.
    pub async fn capture_for_local_scheduling(
        &self,
        watch: &SourceWatch,
        admission: &StandingWatchAdmission,
    ) -> StorageResult<PreparedStandingWatchAuthority> {
        if !matches!(admission.origin(), StandingWatchOrigin::TrustedLocal) {
            return Err(refused());
        }
        let stored = self.stored(watch).await?.ok_or_else(refused)?;
        if !matches!(stored.descriptor.origin, Origin::TrustedLocal) {
            return Err(StorageError::Conflict(
                "standing_watch_requires_registered_dispatch".into(),
            ));
        }
        let original = &stored.descriptor.original_ceiling;
        let selected = admission.principal();
        if original.space_id != selected.space_id
            || original.scopes != selected.scopes
            || original.subject_allowlist != selected.subject_allowlist
            || original.purpose != selected.purpose
            || original.clearances.len() != selected.clearances.len()
            || !original
                .clearances
                .iter()
                .zip(&selected.clearances)
                .all(|(left, right)| {
                    left.policy_id == right.policy_id
                        && left.max_level == right.max_level
                        && left.labels == right.labels
                        && left.handling_allow == right.handling_allow
                })
        {
            return Err(refused());
        }
        self.capture(watch, &stored.identity).await
    }

    /// Capture the current standing registration for a NEW periodic queue
    /// intent.
    ///
    /// Existing attempts must use their immutable identity instead.
    /// # Errors
    /// Refuses absent historical delegation or current revocation.
    pub async fn capture_for_scheduling(
        &self,
        watch: &SourceWatch,
    ) -> StorageResult<PreparedStandingWatchAuthority> {
        let stored = self.stored(watch).await?.ok_or_else(refused)?;
        self.capture(watch, &stored.identity).await
    }

    /// Observe current authority for the exact original queued delegation.
    ///
    /// Never substitutes the current row's identity for a missing/stale input.
    /// # Errors
    /// Refuses replacement, disabled/erased Watch, missing grant or lost scope.
    pub async fn capture(
        &self,
        watch: &SourceWatch,
        identity: &StandingWatchIdentity,
    ) -> StorageResult<PreparedStandingWatchAuthority> {
        let current = crate::SourceWatchRepository::new(self.client.clone())
            .get(&watch.base.space_id, &watch.base.id)
            .await?;
        let stored = self.stored(watch).await?.ok_or_else(refused)?;
        if !active(watch)
            || current.as_ref() != Some(watch)
            || &stored.identity != identity
            || stored.descriptor.watch_intent_fingerprint != semantic_intent(watch)?
        {
            return Err(refused());
        }
        let configuration = self.configuration(&watch.base.space_id).await?;
        let (principal, grant) = match &stored.descriptor.origin {
            Origin::VerifiedService { subject, .. } => {
                let grant = read_grant(&self.client, &watch.base.space_id, subject)
                    .await?
                    .ok_or_else(refused)?;
                let principal =
                    attenuate_current_grant(&stored.descriptor.original_ceiling, subject, &grant)
                        .map_err(|_| refused())?;
                (principal, Some(grant))
            }
            Origin::TrustedLocal => (stored.descriptor.original_ceiling.clone(), None),
        };
        Ok(PreparedStandingWatchAuthority {
            watch: watch.clone(),
            stored,
            configuration,
            grant,
            principal,
        })
    }
}

/// Prepared registration cannot be deserialized or created from wire input.
#[derive(Clone)]
pub struct PreparedStandingWatchRegistration {
    watch: SourceWatch,
    previous_watch: Option<SourceWatch>,
    previous: Option<Stored>,
    next: Stored,
    configuration: SpaceConfig,
    grant: Option<SpaceGrant>,
}

/// A current storage observation bounded by the immutable original ceiling.
#[derive(Clone)]
pub struct PreparedStandingWatchAuthority {
    watch: SourceWatch,
    stored: Stored,
    configuration: SpaceConfig,
    grant: Option<SpaceGrant>,
    principal: Principal,
}

macro_rules! redacted_debug {
    ($name:ty) => {
        impl std::fmt::Debug for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    .field("authority", &"redacted")
                    .finish_non_exhaustive()
            }
        }
    };
}
redacted_debug!(PreparedStandingWatchRegistration);
redacted_debug!(PreparedStandingWatchAuthority);

impl PreparedStandingWatchRegistration {
    /// The exact proposed Watch authorized by this explicit registration.
    #[must_use]
    pub const fn watch(&self) -> &SourceWatch {
        &self.watch
    }

    /// Exact original ceiling which I must authorize against all affected rows.
    #[must_use]
    pub const fn principal(&self) -> &Principal {
        &self.next.descriptor.original_ceiling
    }
    /// Put this original reference into the queue payload before enqueue.
    #[must_use]
    pub const fn identity(&self) -> &StandingWatchIdentity {
        &self.next.identity
    }

    pub(crate) fn check_tx(&self, tx: &mut EmbeddedMutation<'_>) -> StorageResult<()> {
        check_common(
            tx,
            &self.watch,
            self.previous_watch.as_ref(),
            self.previous.as_ref(),
            &self.configuration,
            self.grant.as_ref(),
        )
    }
    /// Call before writing the proposed Watch, within that SAME transaction.
    pub(crate) fn persist_tx(&self, tx: &mut EmbeddedMutation<'_>) -> StorageResult<()> {
        self.check_tx(tx)?;
        tx.upsert_extension_row(TABLE, self.next.embedded())?;
        tx.advance_canonical_epoch(&self.watch.base.space_id)?;
        Ok(())
    }
    pub(crate) fn append_guard_sql(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
    ) -> StorageResult<()> {
        append_common(
            sql,
            bindings,
            &self.watch,
            self.previous_watch.as_ref(),
            self.previous.as_ref(),
            &self.configuration,
            self.grant.as_ref(),
        )
    }
    /// Append before proposed Watch writes; caller owns BEGIN/COMMIT and all
    /// original record/queue guards. Failure rolls back the complete mutation.
    pub(crate) fn append_persist_sql(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
    ) -> StorageResult<()> {
        self.append_guard_sql(sql, bindings)?;
        bindings.insert("standing_watch_next".into(), self.next.native());
        sql.push_str("UPSERT $standing_watch_id CONTENT $standing_watch_next;\n");
        Ok(())
    }
}

impl PreparedStandingWatchAuthority {
    /// Exact current Watch snapshot guarded at the committing mutation.
    #[must_use]
    pub const fn watch(&self) -> &SourceWatch {
        &self.watch
    }

    /// Current scopes intersected with the immutable original full ceiling.
    #[must_use]
    pub const fn principal(&self) -> &Principal {
        &self.principal
    }
    /// Original registration identity, never a current-claim replacement.
    #[must_use]
    pub const fn identity(&self) -> &StandingWatchIdentity {
        &self.stored.identity
    }

    /// Capture for the original Watch/Work/Run already checked by A's producer.
    ///
    /// This proof does not replace A's attempt or original-run checks.
    /// # Errors
    /// Refuses absent/malformed original queue identity or current revocation.
    pub async fn capture_for_original_watch(
        client: &StorageClient,
        watch: &SourceWatch,
        work: &WorkItem,
        run: &WorkRun,
    ) -> StorageResult<Self> {
        if work.base.space_id != watch.base.space_id
            || run.base.space_id != watch.base.space_id
            || run.work_item.id() != &work.base.id
            || work.kind != "refresh_source_watch"
            || work
                .payload
                .get("watch_id")
                .and_then(serde_json::Value::as_str)
                != Some(watch.base.id.to_string().as_str())
        {
            return Err(refused());
        }
        let identity: StandingWatchIdentity =
            serde_json::from_value(work.payload.get(PAYLOAD_KEY).cloned().ok_or_else(refused)?)
                .map_err(invalid)?;
        StandingWatchRepository::new(client.clone())
            .capture(watch, &identity)
            .await
    }

    /// Current authority for a source operation's ORIGINAL historical Watch
    /// producer.
    ///
    /// Cancellation/expiry of the old queue lease does not erase the
    /// accepted operation; current delegation revocation still forbids writes.
    /// # Errors
    /// Refuses missing/replaced private authority or original Work/Run evidence.
    pub async fn capture_for_source_operation(
        client: &StorageClient,
        space: &str,
        fence: &PublicationFence,
    ) -> StorageResult<Option<PreparedStandingSourceAuthority>> {
        source::capture(client, space, fence).await.map(Some)
    }

    pub(crate) fn check_tx(&self, tx: &mut EmbeddedMutation<'_>) -> StorageResult<()> {
        check_common(
            tx,
            &self.watch,
            Some(&self.watch),
            Some(&self.stored),
            &self.configuration,
            self.grant.as_ref(),
        )
    }
    pub(crate) fn append_guard_sql(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
    ) -> StorageResult<()> {
        append_common(
            sql,
            bindings,
            &self.watch,
            Some(&self.watch),
            Some(&self.stored),
            &self.configuration,
            self.grant.as_ref(),
        )
    }
}

// A source/document write advances coverage and bookkeeping in SpaceConfig.
// Those are not policy changes. Keep every other original field, including
// generation, pending reindex, grants' policy envelope and provider bindings.
fn configuration_policy(configuration: &SpaceConfig) -> StorageResult<Value> {
    let Value::Object(mut fields) = crate::canonical_commit::native_expected_fields(
        crate::surreal::space_config_to_value(configuration)?,
    ) else {
        return Err(invalid("standing watch configuration is not an object"));
    };
    fields.remove("updated_at");
    if let Some(Value::Object(processing)) = fields.get_mut("processing") {
        processing.remove("index_generation");
    }
    Ok(Value::Object(fields))
}

fn same_configuration_policy(left: &SpaceConfig, right: &SpaceConfig) -> StorageResult<bool> {
    // The database CONTENT serializer excludes id; native SQL separately checks
    // it before these same policy fields. Preserve that identity here as well.
    Ok(
        left.base.id == right.base.id
            && configuration_policy(left)? == configuration_policy(right)?,
    )
}

#[allow(
    clippy::needless_pass_by_ref_mut,
    reason = "This guard is called within the exclusive mutation protocol; preserve its existing mutable transaction-borrow interface and lifetime through validation."
)]
fn check_common(
    tx: &mut EmbeddedMutation<'_>,
    watch: &SourceWatch,
    previous_watch: Option<&SourceWatch>,
    stored: Option<&Stored>,
    configuration: &SpaceConfig,
    grant: Option<&SpaceGrant>,
) -> StorageResult<()> {
    if tx
        .get::<SourceWatch>(&watch.base.space_id, &watch.base.id, false)?
        .as_ref()
        != previous_watch
    {
        return Err(refused());
    }
    let rows = tx.scan_extension_rows(TABLE)?;
    let row = rows.iter().find(|(key, _)| key == &row_key(&watch.base.id));
    if row.map(|(_, row)| row.clone()) != stored.map(Stored::embedded) {
        return Err(refused());
    }
    let configs: Vec<_> = tx
        .scan::<SpaceConfig>()?
        .into_iter()
        .filter(|value| {
            value.base.space_id == watch.base.space_id && value.base.deleted_at.is_none()
        })
        .collect();
    if configs.len() != 1 || !same_configuration_policy(&configs[0], configuration)? {
        return Err(refused());
    }
    if let Some(grant) = grant {
        let rows = tx.scan_extension_rows("space_grant")?;
        let key = format!(
            "space_grant:{}",
            crate::space_grant_repository::grant_key(&grant.space_id, &grant.subject)
        );
        let payload = grant_payload(grant)?;
        let row = rows.iter().find(|(id, _)| id == &key).ok_or_else(refused)?;
        if row.1["payload"].as_str() != Some(payload.as_str())
            || row.1["space_id"] != grant.space_id
            || row.1["subject"] != grant.subject
        {
            return Err(refused());
        }
    }
    Ok(())
}

fn append_common(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    watch: &SourceWatch,
    previous_watch: Option<&SourceWatch>,
    stored: Option<&Stored>,
    configuration: &SpaceConfig,
    grant: Option<&SpaceGrant>,
) -> StorageResult<()> {
    bindings.extend(crate::bindings! {
        "standing_watch_id"=>row_id(&watch.base.id), "standing_watch_watch"=>crate::surreal::thing_from_record_id(&watch.base.id),
        "standing_watch_space"=>watch.base.space_id.as_str(),
        "standing_watch_old"=>stored.map_or(Value::None, Stored::native),
        "standing_watch_epoch_id"=>crate::source_publication::control_thing("canonical_epoch",&watch.base.space_id),
        "standing_watch_config_id"=>crate::surreal::thing_from_record_id(&configuration.base.id),
        "standing_watch_config"=>configuration_policy(configuration)?,
        "standing_watch_old_watch"=>previous_watch.map(crate::SourceWatchRepository::watch_to_value).transpose()?.map_or(Value::None,crate::canonical_commit::native_expected_fields),
    });
    sql.push_str("LET $standing_watch_current = SELECT * FROM ONLY $standing_watch_id;
        IF $standing_watch_old = NONE {
            IF $standing_watch_current != NONE { THROW 'standing_watch_authority_changed_or_missing'; };
        } ELSE {
            IF $standing_watch_current = NONE { THROW 'standing_watch_authority_changed_or_missing'; };
            FOR $field IN object::entries($standing_watch_old) { IF $standing_watch_current[$field[0]] != $field[1] { THROW 'standing_watch_authority_changed_or_missing'; }; };
            UPDATE $standing_watch_id SET read_nonce=<string>rand::uuid();
        };
        LET $standing_watch_actual_watch = SELECT * FROM ONLY $standing_watch_watch;
        IF $standing_watch_old_watch = NONE {
            IF $standing_watch_actual_watch != NONE { THROW 'standing_watch_authority_changed_or_missing'; };
        } ELSE {
            IF $standing_watch_actual_watch = NONE { THROW 'standing_watch_authority_changed_or_missing'; };
            FOR $field IN object::entries($standing_watch_old_watch) { IF $standing_watch_actual_watch[$field[0]] != $field[1] { THROW 'standing_watch_authority_changed_or_missing'; }; };
            UPDATE $standing_watch_watch SET canonical_read_nonce=<string>rand::uuid();
        };
        LET $standing_watch_configs = SELECT * FROM space_config WHERE space_id=$standing_watch_space AND deleted_at IS NONE;
        IF array::len($standing_watch_configs) != 1 OR $standing_watch_configs[0].id != $standing_watch_config_id { THROW 'standing_watch_authority_changed_or_missing'; };
        FOR $field IN object::entries($standing_watch_config) {
            IF $field[0] = 'processing' AND $field[1] != NONE {
                IF $standing_watch_configs[0].processing = NONE { THROW 'standing_watch_authority_changed_or_missing'; };
                LET $standing_watch_processing_policy = object::from_entries(array::filter(object::entries($standing_watch_configs[0].processing), |$entry| $entry[0] != 'index_generation'));
                IF $standing_watch_processing_policy != $field[1] { THROW 'standing_watch_authority_changed_or_missing'; };
            } ELSE {
                IF $standing_watch_configs[0][$field[0]] != $field[1] { THROW 'standing_watch_authority_changed_or_missing'; };
            };
        };
        LET $standing_watch_epoch=SELECT * FROM ONLY $standing_watch_epoch_id;
        IF $standing_watch_epoch=NONE OR $standing_watch_epoch.space_id!=$standing_watch_space { THROW 'standing_watch_authority_changed_or_missing'; };
        UPDATE $standing_watch_epoch_id SET standing_watch_read_nonce=<string>rand::uuid();\n");
    if let Some(grant) = grant {
        bindings.extend(crate::bindings! { "standing_watch_grant_id"=>grant_id(grant),
            "standing_watch_grant_payload"=>grant_payload(grant)?, "standing_watch_grant_subject"=>grant.subject.as_str() });
        sql.push_str("LET $standing_watch_grant = SELECT * FROM ONLY $standing_watch_grant_id;
            IF $standing_watch_grant = NONE OR $standing_watch_grant.space_id != $standing_watch_space OR $standing_watch_grant.subject != $standing_watch_grant_subject OR $standing_watch_grant.payload != $standing_watch_grant_payload { THROW 'standing_watch_authority_changed_or_missing'; };
");
    }
    Ok(())
}

#[cfg(test)]
#[path = "standing_watch_tests.rs"]
mod tests;

#[path = "standing_watch_source.rs"]
mod source;
pub use source::PreparedStandingSourceAuthority;

pub(crate) fn append_caller_guard(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    guard: &str,
    values: BTreeMap<String, Value>,
) {
    source::append_renamed(sql, bindings, guard, values, "standing_watch_invoker_");
}
