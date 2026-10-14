//! `SpaceConfig` repository implementation for `SurrealDB`.

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;
use std::fmt::Write as _;

use elephant_types::records::SpaceConfig;
use elephant_types::RecordId;

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;
use crate::require::required_field;

pub(crate) const SPACE_CONFIG_PROJECTION: &str = r"
    id,
    space_id,
    created_at,
    updated_at,
    created_by,
    deleted_at,
    security,
    IF provenance IS NOT NONE THEN {
        sources: IF provenance.sources IS NOT NONE THEN array::map(provenance.sources, |$s| string::concat(meta::tb($s), ':', meta::id($s))) ELSE [] END,
        extractor: provenance.extractor,
        run_id: provenance.run_id,
        work_run_id: provenance.work_run_id,
        prompt_id: provenance.prompt_id,
        config_id: provenance.config_id,
        chain: provenance.chain
    } ELSE NONE END AS provenance,
    subjects,
    subjects_state,
    embedding_provider,
    embedding_model,
    embedding_dims,
    purpose,
    skill,
    processing
";

/// Database-only proxy for `SpaceConfig` to avoid flatten/native enum issues.
#[derive(Deserialize)]
pub(crate) struct SpaceConfigDb {
    id: surrealdb::sql::Thing,
    space_id: Option<String>,
    created_at: Option<chrono::DateTime<Utc>>,
    updated_at: Option<chrono::DateTime<Utc>>,
    created_by: Option<String>,
    deleted_at: Option<chrono::DateTime<Utc>>,
    provenance: Option<elephant_types::records::base::Provenance>,
    security: Option<elephant_types::records::base::Security>,
    subjects: Option<Vec<elephant_types::Link<elephant_types::records::entity::Entity>>>,
    subjects_state: Option<elephant_types::records::base::SubjectsState>,
    embedding_provider: Option<String>,
    embedding_model: Option<String>,
    embedding_dims: Option<f64>,
    purpose: Option<String>,
    skill: Option<String>,
    processing: Option<elephant_types::provider_binding::SpaceProcessingConfig>,
}

impl TryFrom<SpaceConfigDb> for SpaceConfig {
    type Error = StorageError;

    fn try_from(db: SpaceConfigDb) -> Result<Self, Self::Error> {
        let embedding_dims = db
            .embedding_dims
            .ok_or_else(|| StorageError::QueryError("Missing embedding_dims".to_string()))?;
        let rounded = embedding_dims.round();
        if (embedding_dims - rounded).abs() > f64::EPSILON
            || rounded < f64::from(i32::MIN)
            || rounded > f64::from(i32::MAX)
        {
            return Err(StorageError::QueryError(format!(
                "Invalid embedding_dims value: {embedding_dims}"
            )));
        }
        #[allow(clippy::cast_possible_truncation)]
        let embedding_dims = rounded as i32;

        Ok(Self {
            base: elephant_types::records::base::BaseFields {
                id: crate::surreal::record_id_from_thing(&db.id)
                    .map_err(|e| StorageError::QueryError(e.to_string()))?,
                space_id: required_field(db.space_id, "space_id")?,
                created_at: db
                    .created_at
                    .ok_or_else(|| StorageError::QueryError("Missing created_at".to_string()))?,
                updated_at: db
                    .updated_at
                    .ok_or_else(|| StorageError::QueryError("Missing updated_at".to_string()))?,
                created_by: required_field(db.created_by, "created_by")?,
                deleted_at: db.deleted_at,
                provenance: required_field(db.provenance, "provenance")?,
                security: required_field(db.security, "security")?,
                subjects: required_field(db.subjects, "subjects")?,
                subjects_state: required_field(db.subjects_state, "subjects_state")?,
            },
            embedding_provider: db.embedding_provider.ok_or_else(|| {
                StorageError::QueryError("Missing embedding_provider".to_string())
            })?,
            embedding_model: db
                .embedding_model
                .ok_or_else(|| StorageError::QueryError("Missing embedding_model".to_string()))?,
            embedding_dims,
            processing: db.processing,
            purpose: db.purpose,
            skill: db.skill,
        })
    }
}

/// `SpaceConfig` repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct SpaceConfigRepository {
    client: StorageClient,
}

/// Space absence/approval failures remain distinguishable from storage outages.
#[derive(Debug, thiserror::Error)]
pub enum SpaceResolutionError {
    /// Storage could not read or validate the current configuration.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// The named space has no current valid approval for the requested operation.
    #[error(transparent)]
    Authorization(#[from] elephant_types::provider_binding::ProviderAuthorizationError),
}

impl SpaceResolutionError {
    /// Preserve the local failure category across provider SDK string-only errors.
    /// These markers describe a failed authorization check, not measured usage.
    #[must_use]
    pub fn dispatch_failure_message(&self) -> String {
        use elephant_types::provider_binding::ProviderAuthorizationError;
        let marker = match self {
            Self::Authorization(ProviderAuthorizationError::WrongSpace) => {
                "recall_dispatch_terminal:"
            }
            Self::Authorization(_) => "configuration_blocked:",
            Self::Storage(
                StorageError::ConnectionError(_)
                | StorageError::TransactionError(_)
                | StorageError::Conflict(_)
                | StorageError::ErasureInProgress,
            ) => "recall_dispatch_transient:",
            Self::Storage(_) => "recall_dispatch_terminal:",
        };
        format!("{marker} provider_authorization: {self}")
    }
}

impl SpaceConfigRepository {
    /// Share this client's verified local model resources, never its approvals.
    #[must_use]
    pub fn local_models(
        &self,
    ) -> std::sync::Arc<elephant_embeddings::local_model_cache::VerifiedLocalModelCache> {
        self.client.local_models()
    }

    /// Resolve an explicit named space. No default-space or process-env fallback.
    ///
    /// # Errors
    /// Absent/deleted spaces, absent policy and malformed authority fail closed.
    pub async fn resolve_space(
        &self,
        space_id: &str,
    ) -> Result<elephant_types::provider_binding::ResolvedSpaceConfig, SpaceResolutionError> {
        use elephant_types::provider_binding::{
            ProviderAuthorizationError, ResolvedSpaceConfig, SpaceConfigToken,
        };
        if space_id.trim().is_empty() {
            return Err(ProviderAuthorizationError::MissingSpace.into());
        }
        let config = self
            .get_by_space_id(space_id)
            .await?
            .ok_or(ProviderAuthorizationError::MissingSpace)?;
        let processing = config
            .processing
            .ok_or(ProviderAuthorizationError::MissingConfiguration)?;
        processing.validate()?;
        Ok(ResolvedSpaceConfig {
            token: SpaceConfigToken {
                space_id: config.base.space_id,
                config_id: config.base.id.to_string(),
                generation: processing.generation,
                policy_id: processing.policy_id.clone(),
            },
            processing,
            purpose: config.purpose,
            skill: config.skill,
        })
    }

    /// Reload current approvals immediately before an outbound request.
    ///
    /// # Errors
    /// Current revocation always takes precedence over pinned job configuration.
    pub async fn authorize_model_operation(
        &self,
        space_id: &str,
        operation: elephant_types::provider_binding::ModelOperation,
        pinned_profile: Option<&str>,
    ) -> Result<elephant_types::provider_binding::ResolvedModelBinding, SpaceResolutionError> {
        Ok(self
            .resolve_space(space_id)
            .await?
            .authorize(space_id, operation, pinned_profile)?)
    }

    /// Replaces a configuration while its authorized envelope is still current.
    ///
    /// # Errors
    ///
    /// Returns a conflict if identity, envelope, lifecycle or version changed.
    pub async fn replace_authorized(
        &self,
        observed: &SpaceConfig,
        proposed: SpaceConfig,
    ) -> StorageResult<SpaceConfig> {
        self.replace_guarded(observed, proposed, false, None).await
    }

    async fn replace_guarded(
        &self,
        observed: &SpaceConfig,
        mut proposed: SpaceConfig,
        reindex_write: bool,
        attempt: Option<&elephant_types::publication::AttemptFence>,
    ) -> StorageResult<SpaceConfig> {
        let conflict = || crate::lifecycle_guard::guard_conflict("SpaceConfig", &observed.base.id);
        if observed.base.id != proposed.base.id
            || observed.base.space_id != proposed.base.space_id
            || observed.base.deleted_at.is_some()
            || !crate::lifecycle_guard::base_envelope_unchanged(&observed.base, &proposed.base)
        {
            return Err(conflict());
        }
        if reindex_write {
            validate_processing(&proposed)?;
            let old = observed.processing.as_ref().ok_or_else(conflict)?;
            let mut ordinary = proposed.clone();
            let next = ordinary.processing.as_mut().ok_or_else(conflict)?;
            next.index_generation = old.index_generation;
            next.pending_reindex.clone_from(&old.pending_reindex);
            next.active_embedding_profile_id
                .clone_from(&old.active_embedding_profile_id);
            old.validate_replacement(next)
                .map_err(|error| StorageError::Conflict(error.to_string()))?;
        } else {
            validate_processing_replacement(observed, &proposed)?;
        }
        proposed.base.updated_at = Utc::now();
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    if let Some(attempt) = attempt {
                        crate::reindex_guard::check(tx, observed, attempt)?;
                    }
                    let current = tx
                        .get::<SpaceConfig>(&observed.base.space_id, &observed.base.id, false)?
                        .ok_or_else(conflict)?;
                    if current.processing != observed.processing
                        || current.base.updated_at != observed.base.updated_at
                        || !crate::lifecycle_guard::base_envelope_unchanged(
                            &current.base,
                            &observed.base,
                        )
                    {
                        return Err(conflict());
                    }
                    if initial_profile_needs_empty(Some(observed), &proposed)
                        && indexed_content_exists(tx, &proposed.base.space_id)?
                    {
                        return Err(StorageError::Conflict(
                            "populated space requires explicit bootstrap reindex before activation"
                                .into(),
                        ));
                    }
                    Ok(tx.replace(proposed)?)
                })
                .await;
        }
        let (condition, mut bindings) =
            crate::lifecycle_guard::base_envelope_condition(&observed.base)?;
        bindings.extend(crate::bindings! {
            "id" => crate::surreal::thing_from_record_id(&observed.base.id),
            "space" => observed.base.space_id.clone(),
            "version" => crate::surreal::datetime_from_chrono(observed.base.updated_at),
            "record" => crate::surreal::space_config_to_value(&proposed)?,
            "initial_active" => initial_profile_needs_empty(Some(observed), &proposed),
            "processing" => match &observed.processing {
                Some(value) => crate::surreal::stored_native_value(crate::surreal::json_to_surreal(serde_json::to_value(value)
                    .map_err(|error| StorageError::SerializationError(error.to_string()))?)?),
                None => surrealdb::sql::Value::None,
            },
        });
        let update = format!("UPDATE $id CONTENT $record WHERE space_id = $space AND updated_at = $version AND processing = $processing AND ($initial_active = false OR ({EMPTY_INDEXED_CONTENT})) AND {condition} RETURN {SPACE_CONFIG_PROJECTION}");
        let sql = if let Some(attempt) = attempt {
            let mut sql = "BEGIN TRANSACTION;\n".to_owned();
            crate::reindex_guard::append(&mut sql, &mut bindings, observed, attempt)?;
            let _ = write!(sql, "LET $reindex_result = ({update});\nIF array::len($reindex_result) != 1 {{ THROW 'reindex configuration changed'; }};\nRETURN $reindex_result;\nCOMMIT TRANSACTION;");
            sql
        } else {
            update
        };
        let mut response = self.client.query_bind_checked(&sql, bindings).await?;
        let result_index = response.num_statements() - 1;
        let rows: Vec<SpaceConfigDb> = response
            .take(result_index)
            .map_err(|error| StorageError::QueryError(error.to_string()))?;
        SpaceConfig::try_from(rows.into_iter().next().ok_or_else(conflict)?)
    }

    /// Stage a target profile while keeping the old profile available for reads.
    /// Caller must hold explicit provider administration; progress is repository-owned.
    pub async fn start_embedding_reindex(
        &self,
        observed: &SpaceConfig,
        target_profile_id: &str,
    ) -> StorageResult<SpaceConfig> {
        use elephant_types::provider_binding::{EmbeddingReindex, ModelOperation};
        let mut proposed = observed.clone();
        let processing = proposed
            .processing
            .as_mut()
            .ok_or_else(|| StorageError::Conflict("processing configuration is required".into()))?;
        if processing.pending_reindex.is_some() {
            return Err(StorageError::Conflict(
                "an embedding reindex is already pending".into(),
            ));
        }
        let from_profile_id = processing.active_embedding_profile_id.clone();
        if from_profile_id.as_deref() == Some(target_profile_id) {
            return Err(StorageError::Conflict(
                "target profile is already active".into(),
            ));
        }
        let target = processing
            .profiles
            .get(target_profile_id)
            .ok_or_else(|| StorageError::Conflict("target profile is not approved".into()))?;
        if target.dimensions.is_none()
            || processing
                .bindings
                .get(&ModelOperation::ReindexEmbedding)
                .is_none_or(|binding| binding.revoked || binding.profile_id != target_profile_id)
            || processing
                .connections
                .get(&target.connection_id)
                .is_none_or(|connection| connection.revoked)
        {
            return Err(StorageError::Conflict(
                "target reindex binding is absent or revoked".into(),
            ));
        }
        processing.pending_reindex = Some(EmbeddingReindex {
            id: uuid::Uuid::now_v7().to_string(),
            from_profile_id,
            to_profile_id: target_profile_id.into(),
            content_generation: processing.index_generation,
            cursor: None,
            embedded_nodes: 0,
            embedded_entities: 0,
            phase: elephant_types::provider_binding::EmbeddingReindexPhase::Nodes,
            complete: false,
        });
        advance_config_generation(processing)?;
        self.replace_guarded(observed, proposed, true, None).await
    }

    /// Restart coverage after any concurrent indexed-content change. Old vectors
    /// remain readable and staged target vectors are safely overwritten on replay.
    pub async fn restart_embedding_reindex(
        &self,
        observed: &SpaceConfig,
    ) -> StorageResult<SpaceConfig> {
        self.restart_embedding_reindex_inner(observed, None).await
    }

    /// Restart coverage only while the original reindex queue claim still owns it.
    ///
    /// # Errors
    /// Rejects stale claims, missing reindex state or a changed configuration.
    pub async fn restart_embedding_reindex_with_attempt(
        &self,
        observed: &SpaceConfig,
        attempt: &elephant_types::publication::AttemptFence,
    ) -> StorageResult<SpaceConfig> {
        self.restart_embedding_reindex_inner(observed, Some(attempt))
            .await
    }

    async fn restart_embedding_reindex_inner(
        &self,
        observed: &SpaceConfig,
        attempt: Option<&elephant_types::publication::AttemptFence>,
    ) -> StorageResult<SpaceConfig> {
        let mut proposed = observed.clone();
        let processing = proposed
            .processing
            .as_mut()
            .ok_or_else(|| StorageError::Conflict("processing configuration is required".into()))?;
        let pending = processing
            .pending_reindex
            .as_mut()
            .ok_or_else(|| StorageError::Conflict("no reindex pending".into()))?;
        pending.content_generation = processing.index_generation;
        pending.cursor = None;
        pending.embedded_nodes = 0;
        pending.embedded_entities = 0;
        pending.phase = elephant_types::provider_binding::EmbeddingReindexPhase::Nodes;
        pending.complete = false;
        advance_config_generation(processing)?;
        self.replace_guarded(observed, proposed, true, attempt)
            .await
    }

    /// Record progress only after every vector in the page is durable. A crashed
    /// write before this CAS repeats the page; progress never leads durable data.
    pub async fn record_embedding_reindex_page(
        &self,
        observed: &SpaceConfig,
        cursor: Option<String>,
        embedded_nodes: u64,
        complete: bool,
    ) -> StorageResult<SpaceConfig> {
        self.record_embedding_reindex_page_inner(observed, cursor, embedded_nodes, complete, None)
            .await
    }

    /// Advance durable coverage under the original queue claim in the same CAS.
    ///
    /// # Errors
    /// Rejects stale claims/coverage, non-advancing cursors and counter overflow.
    pub async fn record_embedding_reindex_page_with_attempt(
        &self,
        observed: &SpaceConfig,
        cursor: Option<String>,
        embedded_nodes: u64,
        complete: bool,
        attempt: &elephant_types::publication::AttemptFence,
    ) -> StorageResult<SpaceConfig> {
        self.record_embedding_reindex_page_inner(
            observed,
            cursor,
            embedded_nodes,
            complete,
            Some(attempt),
        )
        .await
    }

    async fn record_embedding_reindex_page_inner(
        &self,
        observed: &SpaceConfig,
        cursor: Option<String>,
        embedded_nodes: u64,
        complete: bool,
        attempt: Option<&elephant_types::publication::AttemptFence>,
    ) -> StorageResult<SpaceConfig> {
        use elephant_types::provider_binding::EmbeddingReindexPhase;
        let mut proposed = observed.clone();
        let processing = proposed
            .processing
            .as_mut()
            .ok_or_else(|| StorageError::Conflict("processing configuration is required".into()))?;
        let pending = processing
            .pending_reindex
            .as_mut()
            .ok_or_else(|| StorageError::Conflict("no reindex pending".into()))?;
        if pending.content_generation != processing.index_generation || pending.complete {
            return Err(StorageError::Conflict(
                "reindex coverage is stale or already complete".into(),
            ));
        }
        if cursor
            .as_ref()
            .zip(pending.cursor.as_ref())
            .is_some_and(|(next, old)| next <= old)
        {
            return Err(StorageError::Conflict("reindex cursor must advance".into()));
        }
        pending.cursor = cursor.or_else(|| pending.cursor.clone());
        let count = match pending.phase {
            EmbeddingReindexPhase::Nodes => &mut pending.embedded_nodes,
            EmbeddingReindexPhase::Entities => &mut pending.embedded_entities,
        };
        *count = count
            .checked_add(embedded_nodes)
            .ok_or_else(|| StorageError::DataIntegrityError("reindex count overflow".into()))?;
        if complete {
            match pending.phase {
                EmbeddingReindexPhase::Nodes => {
                    pending.phase = EmbeddingReindexPhase::Entities;
                    pending.cursor = None;
                }
                EmbeddingReindexPhase::Entities => pending.complete = true,
            }
        }
        advance_config_generation(processing)?;
        self.replace_guarded(observed, proposed, true, attempt)
            .await
    }

    /// Atomically activate only a fully covered, unchanged space. Content writers
    /// bump the same config row, so a concurrent mutation conflicts with activation.
    pub async fn activate_embedding_reindex(
        &self,
        observed: &SpaceConfig,
        reindex_id: &str,
    ) -> StorageResult<SpaceConfig> {
        use elephant_types::provider_binding::ModelOperation;
        let mut proposed = observed.clone();
        let processing = proposed
            .processing
            .as_mut()
            .ok_or_else(|| StorageError::Conflict("processing configuration is required".into()))?;
        let pending = processing
            .pending_reindex
            .clone()
            .ok_or_else(|| StorageError::Conflict("no reindex pending".into()))?;
        if pending.id != reindex_id
            || !pending.complete
            || pending.phase != elephant_types::provider_binding::EmbeddingReindexPhase::Entities
            || pending.content_generation != processing.index_generation
        {
            return Err(StorageError::Conflict(
                "reindex is incomplete, stale or superseded".into(),
            ));
        }
        let binding = processing
            .bindings
            .get(&ModelOperation::ReindexEmbedding)
            .ok_or_else(|| StorageError::Conflict("reindex approval missing".into()))?;
        let profile = processing
            .profiles
            .get(&pending.to_profile_id)
            .ok_or_else(|| StorageError::Conflict("target profile missing".into()))?;
        let connection = processing
            .connections
            .get(&profile.connection_id)
            .ok_or_else(|| StorageError::Conflict("target connection missing".into()))?;
        if binding.revoked || binding.profile_id != profile.id || connection.revoked {
            return Err(StorageError::Conflict("reindex approval revoked".into()));
        }
        proposed.embedding_model.clone_from(&profile.model);
        proposed.embedding_provider.clone_from(&connection.provider);
        proposed.embedding_dims =
            i32::try_from(profile.dimensions.ok_or_else(|| {
                StorageError::Conflict("target is not an embedding profile".into())
            })?)
            .map_err(|_| {
                StorageError::DataIntegrityError("embedding dimensions overflow".into())
            })?;
        // Existing query/document grants must remain approved. Only their profile
        // changes, in the same commit as activation; revocation is never undone.
        for operation in [
            ModelOperation::QueryEmbedding,
            ModelOperation::DocumentEmbedding,
        ] {
            let operation_binding = processing
                .bindings
                .get_mut(&operation)
                .ok_or_else(|| StorageError::Conflict("query/document binding missing".into()))?;
            if operation_binding.revoked {
                return Err(StorageError::Conflict(
                    "query/document binding revoked".into(),
                ));
            }
            operation_binding
                .profile_id
                .clone_from(&pending.to_profile_id);
        }
        processing.active_embedding_profile_id = Some(pending.to_profile_id);
        processing.pending_reindex = None;
        advance_config_generation(processing)?;
        self.replace_guarded(observed, proposed, true, None).await
    }

    /// Cancel a staged change without replacing or deleting the active partition.
    pub async fn cancel_embedding_reindex(
        &self,
        observed: &SpaceConfig,
    ) -> StorageResult<SpaceConfig> {
        let mut proposed = observed.clone();
        let processing = proposed
            .processing
            .as_mut()
            .ok_or_else(|| StorageError::Conflict("processing configuration is required".into()))?;
        processing.pending_reindex = None;
        advance_config_generation(processing)?;
        self.replace_guarded(observed, proposed, true, None).await
    }

    /// Creates a new space config repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Gets the space config for a given space.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn get_by_space_id(&self, space_id: &str) -> StorageResult<Option<SpaceConfig>> {
        if let Some(store) = self.client.embedded_store() {
            let mut configs = store.scan::<SpaceConfig>().await?;
            configs.retain(|config| {
                config.base.space_id == space_id && config.base.deleted_at.is_none()
            });
            configs.sort_by_key(|config| config.base.id.to_string());
            return Ok(configs.into_iter().next());
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {SPACE_CONFIG_PROJECTION} FROM space_config WHERE space_id = $space_id AND deleted_at IS NONE LIMIT 1"
                ),
                crate::bindings! { "space_id" => space_id },
            )
            .await?;

        let configs: Vec<SpaceConfigDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse space_config: {e}")))?;

        configs
            .into_iter()
            .next()
            .map(SpaceConfig::try_from)
            .transpose()
    }

    /// Lists all space configs across all spaces (admin use).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn list_all(
        &self,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<SpaceConfig>> {
        if let Some(store) = self.client.embedded_store() {
            let mut configs = store.scan::<SpaceConfig>().await?;
            configs.retain(|config| config.base.deleted_at.is_none());
            configs.sort_by_key(|config| config.base.id.to_string());
            if let Some(cursor) = cursor {
                let cursor_id = cursor.to_string();
                configs.retain(|config| config.base.id.to_string() > cursor_id);
            }
            if configs.len() > limit {
                configs.truncate(limit);
            }
            return Ok(configs);
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {SPACE_CONFIG_PROJECTION} FROM space_config WHERE deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {SPACE_CONFIG_PROJECTION} FROM space_config WHERE deleted_at IS NONE ORDER BY id LIMIT {limit}"
            )
        };

        let mut response = self
            .client
            .query_bind(
                &query,
                crate::bindings! {
                    "cursor" => cursor.map_or(surrealdb::sql::Value::None, |r| {
                        surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(r))
                    }),
                },
            )
            .await?;

        let configs: Vec<SpaceConfigDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse space_config list: {e}"))
        })?;

        configs.into_iter().map(SpaceConfig::try_from).collect()
    }
}

#[async_trait]
impl Repository<SpaceConfig> for SpaceConfigRepository {
    async fn create(&self, record: SpaceConfig) -> StorageResult<SpaceConfig> {
        validate_processing(&record)?;
        validate_initial_processing(&record)?;
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let duplicate = mutation
                        .scan::<SpaceConfig>()?
                        .into_iter()
                        .any(|existing| existing.base.space_id == record.base.space_id);
                    if duplicate {
                        return Err(StorageError::Conflict(format!(
                            "Duplicate space_config for space {}",
                            record.base.space_id
                        )));
                    }

                    if initial_profile_needs_empty(None, &record)
                        && indexed_content_exists(mutation, &record.base.space_id)?
                    {
                        return Err(StorageError::Conflict(
                            "populated space requires explicit bootstrap reindex before activation"
                                .into(),
                        ));
                    }
                    Ok(mutation.create(record.clone())?)
                })
                .await;
        }

        if initial_profile_needs_empty(None, &record) {
            self.client.query_bind_checked(
                &format!("BEGIN TRANSACTION; IF ({EMPTY_INDEXED_CONTENT}) = false {{ THROW 'populated space requires explicit bootstrap reindex before activation'; }}; CREATE $id CONTENT $record; COMMIT TRANSACTION;"),
                crate::bindings! { "space" => record.base.space_id.clone(), "id" => crate::surreal::thing_from_record_id(&record.base.id), "record" => crate::surreal::space_config_to_value(&record)? },
            ).await?;
            return Ok(record);
        }

        // Checked single round-trip: statement errors (including duplicate ids)
        // are classified via classify_query_error inside query_bind_checked, so
        // a duplicate CREATE surfaces as StorageError::Conflict instead of
        // silently returning the pre-existing row.
        let mut response = self
            .client
            .query_bind_checked(
                &format!("CREATE $id CONTENT $record RETURN {SPACE_CONFIG_PROJECTION}"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => crate::surreal::space_config_to_value(&record)?,
                },
            )
            .await?;

        let configs: Vec<SpaceConfigDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created space_config: {e}"))
        })?;

        let config_db = configs.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but space_config not found".to_string())
        })?;

        SpaceConfig::try_from(config_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<SpaceConfig>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<SpaceConfig>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {SPACE_CONFIG_PROJECTION} FROM space_config WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let configs: Vec<SpaceConfigDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse space_config: {e}")))?;

        Ok(configs
            .into_iter()
            .next()
            .map(SpaceConfig::try_from)
            .transpose()?)
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: SpaceConfig,
    ) -> StorageResult<SpaceConfig> {
        let observed = self
            .get(space_id, id)
            .await?
            .ok_or_else(|| StorageError::NotFound(id.to_string()))?;
        self.replace_authorized(&observed, record).await
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<SpaceConfig>(space_id, id).await;
        }

        self.client
            .query_bind_checked(
                "UPDATE $id SET deleted_at = time::now(), updated_at = time::now() WHERE space_id = $space_id",
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        Ok(())
    }

    async fn list(
        &self,
        _space_id: &str,
        _limit: usize,
        _cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<SpaceConfig>> {
        Err(StorageError::QueryError(
            "List not supported for space_config".to_string(),
        ))
    }
}

#[async_trait]
impl elephant_embeddings::governed::EmbeddingBindingResolver for SpaceConfigRepository {
    async fn resolve(
        &self,
        space_id: &str,
        operation: elephant_types::provider_binding::ModelOperation,
        pinned_profile: &str,
    ) -> elephant_embeddings::EmbeddingResult<elephant_types::provider_binding::ResolvedModelBinding>
    {
        self.authorize_model_operation(space_id, operation, Some(pinned_profile))
            .await
            .map_err(|error| {
                elephant_embeddings::EmbeddingError::ProviderError(error.dispatch_failure_message())
            })
    }
}

fn advance_config_generation(
    processing: &mut elephant_types::provider_binding::SpaceProcessingConfig,
) -> StorageResult<()> {
    processing.generation = processing.generation.checked_add(1).ok_or_else(|| {
        StorageError::DataIntegrityError("configuration generation exhausted".into())
    })?;
    Ok(())
}

const EMPTY_INDEXED_CONTENT: &str = "array::len((SELECT VALUE id FROM source WHERE space_id = $space AND deleted_at IS NONE LIMIT 1)) = 0 AND array::len((SELECT VALUE id FROM doc WHERE space_id = $space AND deleted_at IS NONE LIMIT 1)) = 0 AND array::len((SELECT VALUE id FROM doc_node WHERE space_id = $space AND deleted_at IS NONE LIMIT 1)) = 0 AND array::len((SELECT VALUE id FROM entity WHERE space_id = $space AND deleted_at IS NONE LIMIT 1)) = 0";

fn initial_profile_needs_empty(observed: Option<&SpaceConfig>, proposed: &SpaceConfig) -> bool {
    observed.is_none_or(|old| old.processing.is_none())
        && proposed
            .processing
            .as_ref()
            .is_some_and(|processing| processing.active_embedding_profile_id.is_some())
}

fn indexed_content_exists(
    tx: &crate::embedded::EmbeddedMutation<'_>,
    space: &str,
) -> StorageResult<bool> {
    use elephant_types::records::{Doc, DocNode, Entity, Source};
    Ok(tx
        .scan::<Source>()?
        .iter()
        .any(|record| record.base.space_id == space && record.base.deleted_at.is_none())
        || tx
            .scan::<Doc>()?
            .iter()
            .any(|record| record.base.space_id == space && record.base.deleted_at.is_none())
        || tx
            .scan::<DocNode>()?
            .iter()
            .any(|record| record.base.space_id == space && record.base.deleted_at.is_none())
        || tx
            .scan::<Entity>()?
            .iter()
            .any(|record| record.base.space_id == space && record.base.deleted_at.is_none()))
}

fn validate_initial_processing(record: &SpaceConfig) -> StorageResult<()> {
    if record.processing.as_ref().is_some_and(|processing| {
        processing.generation != 1
            || processing.index_generation != 0
            || processing.pending_reindex.is_some()
    }) {
        return Err(StorageError::Conflict(
            "initial processing authority cannot supply repository-owned progress".into(),
        ));
    }
    Ok(())
}

fn validate_processing(record: &SpaceConfig) -> StorageResult<()> {
    if let Some(processing) = &record.processing {
        processing
            .validate()
            .map_err(|error| StorageError::DataIntegrityError(error.to_string()))?;
        if let Some(profile_id) = &processing.active_embedding_profile_id {
            let profile = processing
                .profiles
                .get(profile_id)
                .ok_or_else(|| StorageError::DataIntegrityError("active profile missing".into()))?;
            let connection = processing
                .connections
                .get(&profile.connection_id)
                .ok_or_else(|| {
                    StorageError::DataIntegrityError("active connection missing".into())
                })?;
            if record.embedding_provider != connection.provider
                || record.embedding_model != profile.model
                || usize::try_from(record.embedding_dims).ok() != profile.dimensions
            {
                return Err(StorageError::DataIntegrityError(
                    "legacy embedding fields disagree with active profile".into(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_processing_replacement(
    observed: &SpaceConfig,
    proposed: &SpaceConfig,
) -> StorageResult<()> {
    validate_processing(proposed)?;
    if observed.processing == proposed.processing {
        return Ok(());
    }
    match (&observed.processing, &proposed.processing) {
        (Some(old), Some(next)) => old
            .validate_replacement(next)
            .map_err(|error| StorageError::Conflict(error.to_string())),
        (None, Some(_)) => validate_initial_processing(proposed),
        _ => Err(StorageError::Conflict(
            "processing authority cannot be removed or reset".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::mixed_attributes_style,
        clippy::similar_names,
        clippy::type_complexity,
        clippy::float_cmp,
        clippy::const_is_empty,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    use super::*;
    use crate::{Repository, StorageClient};
    use elephant_types::ids::TableName;
    use elephant_types::records::base::{Provenance, Security, SubjectsState};

    async fn setup_embedded_repo() -> SpaceConfigRepository {
        let client = StorageClient::new_embedded()
            .await
            .expect("embedded client");
        SpaceConfigRepository::new(client)
    }

    fn create_test_space_config(space_id: &str) -> SpaceConfig {
        let mut base = elephant_types::records::base::BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            space_id.to_string(),
            "system".to_string(),
        );
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        SpaceConfig {
            processing: None,
            base,
            embedding_provider: "fake".to_string(),
            embedding_model: "test-model".to_string(),
            embedding_dims: 3,
            purpose: None,
            skill: None,
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_space_config_roundtrip() {
        let client = StorageClient::new_memory().await.expect("mem client");
        let repo = SpaceConfigRepository::new(client);

        let mut base = elephant_types::records::base::BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            "space_test".to_string(),
            "system".to_string(),
        );
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        let cfg = SpaceConfig {
            processing: None,
            base: base.clone(),
            embedding_provider: "fake".to_string(),
            embedding_model: "test-model".to_string(),
            embedding_dims: 3,
            purpose: None,
            skill: None,
        };

        let created = repo.create(cfg).await.expect("create config");
        assert_eq!(created.embedding_model, "test-model");

        let fetched = repo
            .get_by_space_id("space_test")
            .await
            .expect("get config")
            .expect("missing config");
        assert_eq!(fetched.embedding_dims, 3);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_space_config_purpose_roundtrip() {
        let client = StorageClient::new_memory().await.expect("mem client");
        let repo = SpaceConfigRepository::new(client);

        let mut base = elephant_types::records::base::BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            "space_purpose_test".to_string(),
            "system".to_string(),
        );
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        let cfg = SpaceConfig {
            processing: None,
            base,
            embedding_provider: "fake".to_string(),
            embedding_model: "test-model".to_string(),
            embedding_dims: 3,
            purpose: Some("Track project relationships".to_string()),
            skill: None,
        };

        let created = repo.create(cfg).await.expect("create config");
        assert_eq!(
            created.purpose,
            Some("Track project relationships".to_string())
        );

        let fetched = repo
            .get_by_space_id("space_purpose_test")
            .await
            .expect("get config")
            .expect("missing config");
        assert_eq!(
            fetched.purpose,
            Some("Track project relationships".to_string())
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_embedded_create_rejects_duplicate_space_id() {
        let repo = setup_embedded_repo().await;

        repo.create(create_test_space_config("space_test"))
            .await
            .expect("create first config");
        let err = repo
            .create(create_test_space_config("space_test"))
            .await
            .expect_err("duplicate space_id should conflict");
        assert!(
            matches!(err, StorageError::Conflict(msg) if msg.contains("Duplicate space_config"))
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_embedded_store_rejects_duplicate_space_id_direct_insert() {
        let repo = setup_embedded_repo().await;
        repo.create(create_test_space_config("space_a"))
            .await
            .expect("create first config");

        let second = create_test_space_config("space_a");
        let err = repo
            .client
            .embedded_store()
            .expect("embedded store")
            .create(second.clone())
            .await
            .expect_err("direct duplicate insert should conflict");
        assert!(
            matches!(err, StorageError::Conflict(msg) if msg.contains("Duplicate space_config"))
        );
    }
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn exercise_authorized_space_replacement(client: StorageClient) {
        let repo = SpaceConfigRepository::new(client.clone());
        let initial = repo
            .create(create_test_space_config("alpha"))
            .await
            .unwrap();
        let mut proposed = initial.clone();
        proposed.purpose = Some("new purpose".into());
        let saved = repo
            .replace_authorized(&initial, proposed.clone())
            .await
            .unwrap();
        assert_eq!(saved.purpose, proposed.purpose);
        assert!(matches!(
            repo.replace_authorized(&initial, proposed).await,
            Err(StorageError::Conflict(_))
        ));
        let mut protected = saved.clone();
        protected.base.security.level = 5;
        // Inject a concurrent administrative reclassification below the
        // ordinary repository boundary, which deliberately forbids relabels.
        if let Some(store) = client.embedded_store() {
            store
                .update_existing("alpha", &saved.base.id, protected.clone())
                .await
                .unwrap();
        } else {
            client
                .query_bind_checked(
                    "UPDATE $id SET security.level = 5",
                    crate::bindings! { "id" => crate::surreal::thing_from_record_id(&saved.base.id) },
                )
                .await
                .unwrap();
        }
        assert!(matches!(
            repo.replace_authorized(&saved, saved.clone()).await,
            Err(StorageError::Conflict(_))
        ));
        assert_eq!(
            repo.get("alpha", &saved.base.id)
                .await
                .unwrap()
                .unwrap()
                .base
                .security,
            protected.base.security
        );
    }

    #[tokio::test]
    async fn authorized_space_replacement_embedded() {
        exercise_authorized_space_replacement(StorageClient::new_embedded().await.unwrap()).await;
    }

    #[tokio::test]
    async fn authorized_space_replacement_surreal() {
        exercise_authorized_space_replacement(StorageClient::new_memory().await.unwrap()).await;
    }
    fn approved_config(space: &str) -> SpaceConfig {
        use elephant_types::provider_binding::*;
        use std::collections::BTreeMap;
        let mut config = create_test_space_config(space);
        config.embedding_provider = "openai".into();
        config.embedding_model = "embed-v1".into();
        let connection = ProviderConnection {
            id: "account".into(),
            provider: "openai".into(),
            account_id: format!("account-{space}"),
            endpoint: "https://example.test/v1".into(),
            credential_ref: Some("TEST_ONLY".into()),
            credential_generation: 1,
            revoked: false,
        };
        let profile = ModelProfile {
            id: "v1".into(),
            connection_id: "account".into(),
            model: "embed-v1".into(),
            model_version: "1".into(),
            dimensions: Some(3),
            preprocessing: "text-v1".into(),
        };
        let mut target = profile.clone();
        target.id = "v2".into();
        target.model = "embed-v2".into();
        target.model_version = "2".into();
        target.dimensions = Some(4);
        config.processing = Some(SpaceProcessingConfig {
            generation: 1,
            index_generation: 0,
            policy_id: "processor-policy".into(),
            connections: BTreeMap::from([("account".into(), connection)]),
            profiles: BTreeMap::from([("v1".into(), profile), ("v2".into(), target)]),
            bindings: BTreeMap::from([
                (
                    ModelOperation::QueryEmbedding,
                    OperationBinding {
                        id: "query".into(),
                        profile_id: "v1".into(),
                        revoked: false,
                    },
                ),
                (
                    ModelOperation::DocumentEmbedding,
                    OperationBinding {
                        id: "document".into(),
                        profile_id: "v1".into(),
                        revoked: false,
                    },
                ),
                (
                    ModelOperation::ReindexEmbedding,
                    OperationBinding {
                        id: "reindex".into(),
                        profile_id: "v2".into(),
                        revoked: false,
                    },
                ),
            ]),
            active_embedding_profile_id: Some("v1".into()),
            pending_reindex: None,
        });
        config
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_in_scrutinee,
        reason = "Construct both backend fixtures before the loop and retain their original teardown order."
    )]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn provider_authority_roundtrips_and_revocation_is_current_on_both_backends() {
        use elephant_types::provider_binding::{ModelOperation, ProviderAuthorizationError};
        for client in [
            StorageClient::new_memory().await.unwrap(),
            StorageClient::new_embedded().await.unwrap(),
        ] {
            let repo = SpaceConfigRepository::new(client);
            let config = repo.create(approved_config("named-a")).await.unwrap();
            let binding = repo
                .authorize_model_operation("named-a", ModelOperation::QueryEmbedding, None)
                .await
                .unwrap();
            assert_eq!(binding.connection.account_id, "account-named-a");
            assert!(matches!(
                repo.authorize_model_operation("named-b", ModelOperation::QueryEmbedding, None)
                    .await,
                Err(SpaceResolutionError::Authorization(
                    ProviderAuthorizationError::MissingSpace
                ))
            ));
            let mut revoked = config.clone();
            let processing = revoked.processing.as_mut().unwrap();
            processing.generation += 1;
            processing.connections.get_mut("account").unwrap().revoked = true;
            repo.replace_authorized(&config, revoked).await.unwrap();
            assert!(matches!(
                repo.authorize_model_operation(
                    "named-a",
                    ModelOperation::QueryEmbedding,
                    Some("v1")
                )
                .await,
                Err(SpaceResolutionError::Authorization(
                    ProviderAuthorizationError::Revoked
                ))
            ));
            assert!(repo
                .replace_authorized(&config, config.clone())
                .await
                .is_err());
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_in_scrutinee,
        reason = "Construct both backend fixtures before the loop and retain their original teardown order."
    )]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn reindex_activation_requires_full_current_coverage_on_both_backends() {
        use elephant_types::provider_binding::ModelOperation;
        use elephant_types::records::{Source, SourceType};
        for client in [
            StorageClient::new_memory().await.unwrap(),
            StorageClient::new_embedded().await.unwrap(),
        ] {
            if client.embedded_store().is_none() {
                client
                    .query_bind_checked(
                        include_str!("../../migrations/sql/033_space_processing.surql"),
                        crate::bindings! {},
                    )
                    .await
                    .unwrap();
            }
            let repo = SpaceConfigRepository::new(client.clone());
            let original = repo.create(approved_config("named-a")).await.unwrap();
            let mut implicit = original.clone();
            let processing = implicit.processing.as_mut().unwrap();
            processing.generation += 1;
            processing.active_embedding_profile_id = Some("v2".into());
            implicit.embedding_model = "embed-v2".into();
            implicit.embedding_dims = 4;
            assert!(repo.replace_authorized(&original, implicit).await.is_err());
            let started = repo.start_embedding_reindex(&original, "v2").await.unwrap();
            let id = started
                .processing
                .as_ref()
                .unwrap()
                .pending_reindex
                .as_ref()
                .unwrap()
                .id
                .clone();
            assert!(repo
                .activate_embedding_reindex(&started, &id)
                .await
                .is_err());
            assert_eq!(
                repo.authorize_model_operation("named-a", ModelOperation::QueryEmbedding, None)
                    .await
                    .unwrap()
                    .profile
                    .id,
                "v1"
            );
            assert_eq!(
                repo.authorize_model_operation("named-a", ModelOperation::ReindexEmbedding, None)
                    .await
                    .unwrap()
                    .profile
                    .id,
                "v2"
            );
            // Empty inventory can finish without a model call, but a subsequent
            // content mutation invalidates even that complete durable pass.
            let ready = repo
                .record_embedding_reindex_page(&started, None, 0, true)
                .await
                .unwrap();
            assert!(
                !ready
                    .processing
                    .as_ref()
                    .unwrap()
                    .pending_reindex
                    .as_ref()
                    .unwrap()
                    .complete
            );
            assert!(repo.activate_embedding_reindex(&ready, &id).await.is_err());
            let ready = repo
                .record_embedding_reindex_page(&ready, None, 0, true)
                .await
                .unwrap();
            let source = Source::new(
                elephant_types::records::base::BaseFields::new(
                    RecordId::new_v4(TableName::Source),
                    "named-a".into(),
                    "test".into(),
                ),
                SourceType::Manual,
                "hash".into(),
            );
            crate::SourceRepository::new(client.clone())
                .create(source)
                .await
                .unwrap();
            assert!(repo.activate_embedding_reindex(&ready, &id).await.is_err());
            let changed = repo.get_by_space_id("named-a").await.unwrap().unwrap();
            assert!(
                changed.processing.as_ref().unwrap().index_generation
                    > ready.processing.as_ref().unwrap().index_generation
            );
            assert!(repo
                .activate_embedding_reindex(&changed, &id)
                .await
                .is_err());
            let restarted = repo.restart_embedding_reindex(&changed).await.unwrap();
            let ready = repo
                .record_embedding_reindex_page(&restarted, None, 0, true)
                .await
                .unwrap();
            let ready = repo
                .record_embedding_reindex_page(&ready, None, 0, true)
                .await
                .unwrap();
            let active = repo.activate_embedding_reindex(&ready, &id).await.unwrap();
            assert_eq!(active.embedding_model, "embed-v2");
            assert_eq!(active.embedding_dims, 4);
            assert!(active.processing.unwrap().pending_reindex.is_none());
            assert_eq!(
                repo.authorize_model_operation("named-a", ModelOperation::QueryEmbedding, None)
                    .await
                    .unwrap()
                    .profile
                    .id,
                "v2"
            );
            assert!(repo
                .authorize_model_operation("named-a", ModelOperation::QueryEmbedding, Some("v1"))
                .await
                .is_err());
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_in_scrutinee,
        reason = "Construct both backend fixtures before the loop and retain their original teardown order."
    )]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep configs, jobs, nodes, vectors in the original fixture scope through assertions and cleanup."
    )]
    async fn reindex_original_claim_fences_vectors_and_progress_on_both_backends() {
        use crate::entity_profile_embedding_repository::{
            entity_input_hash, EntityProfileEmbeddingRepository,
        };
        use crate::work_attempt_repository::fence_from_claim;
        use crate::{EntityRepository, WorkItemRepository};
        use elephant_embeddings::governed::policy_for_binding;
        use elephant_types::provider_binding::ModelOperation;
        use elephant_types::records::base::BaseFields;
        use elephant_types::records::entity::EntityType;
        use elephant_types::records::{Entity, WorkItem, WorkItemStatus};
        for client in [
            StorageClient::new_memory().await.unwrap(),
            StorageClient::new_embedded().await.unwrap(),
        ] {
            if client.embedded_store().is_none() {
                for migration in [
                    include_str!("../../migrations/sql/031_source_publication.surql"),
                    include_str!("../../migrations/sql/032_work_recovery.surql"),
                    include_str!("../../migrations/sql/033_space_processing.surql"),
                ] {
                    client
                        .query_bind_checked(migration, crate::bindings! {})
                        .await
                        .unwrap();
                }
            }
            let configs = SpaceConfigRepository::new(client.clone());
            configs
                .create(approved_config("claim-fenced"))
                .await
                .unwrap();
            let entity = EntityRepository::new(client.clone())
                .create(Entity::new(
                    BaseFields::new(
                        RecordId::new_v4(TableName::Entity),
                        "claim-fenced".into(),
                        "test".into(),
                    ),
                    EntityType::new("person"),
                    "Current input".into(),
                ))
                .await
                .unwrap();
            let node = crate::DocNodeRepository::new(client.clone())
                .create(elephant_types::records::DocNode {
                    base: BaseFields::new(
                        RecordId::new_v4(TableName::DocNode),
                        "claim-fenced".into(),
                        "test".into(),
                    ),
                    revision: elephant_types::Link::new(RecordId::new_v4(TableName::DocRevision)),
                    role: elephant_types::records::doc_node::NodeRole::Chunk,
                    span: elephant_types::records::doc_node::TextSpan::new(0, 5),
                    node_key: "fenced-node".into(),
                    text: Some("hello".into()),
                    parent: None,
                    position: 0,
                    depth: 0,
                    embedding_model: None,
                    collection: None,
                })
                .await
                .unwrap();
            let original = configs
                .get_by_space_id("claim-fenced")
                .await
                .unwrap()
                .unwrap();
            let staged = configs
                .start_embedding_reindex(&original, "v2")
                .await
                .unwrap();
            let reindex_id = staged
                .processing
                .as_ref()
                .unwrap()
                .pending_reindex
                .as_ref()
                .unwrap()
                .id
                .clone();
            let jobs = WorkItemRepository::new(client.clone());
            let mut work = WorkItem {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkItem),
                    "claim-fenced".into(),
                    "test".into(),
                ),
                kind: "reindex_space_embeddings".into(),
                payload: serde_json::json!({"reindex_id": reindex_id}),
                status: WorkItemStatus::Runnable,
                priority: 0,
                idempotency_key: uuid::Uuid::new_v4().to_string(),
                attempts: 0,
                lease_expires_at: None,
                leased_by: None,
                run_after: None,
                last_error: None,
            };
            let created = jobs.create(work.clone()).await.unwrap();
            let claim = jobs
                .claim_with_lease(
                    "claim-fenced",
                    &created.base.id,
                    "first",
                    Utc::now() + chrono::Duration::minutes(5),
                )
                .await
                .unwrap()
                .unwrap();
            let first = fence_from_claim(&claim).unwrap();
            crate::reindex_guard::validate_attempt(&client, &staged, &first)
                .await
                .unwrap();
            let policy = policy_for_binding(
                &configs
                    .authorize_model_operation(
                        "claim-fenced",
                        ModelOperation::ReindexEmbedding,
                        None,
                    )
                    .await
                    .unwrap(),
            )
            .unwrap();
            let nodes = crate::NodeEmbeddingRepository::new(
                client.clone(),
                elephant_embeddings::PartitionManager::new(),
            );
            let embedding = elephant_types::records::NodeEmbedding {
                base: node.base.clone(),
                node: elephant_types::Link::new(node.base.id.clone()),
                model: policy.model.clone(),
                dims: 4,
                vector: vec![1.0, 0.0, 0.0, 0.0],
            };
            nodes
                .store_reindex_embeddings_with_attempt(&staged, vec![embedding.clone()], &first)
                .await
                .unwrap();
            assert!(crate::DocNodeRepository::new(client.clone())
                .get("claim-fenced", &node.base.id)
                .await
                .unwrap()
                .unwrap()
                .embedding_model
                .is_none());
            let vectors = EntityProfileEmbeddingRepository::new(client.clone());
            vectors
                .store_with_attempt(
                    &staged,
                    &entity,
                    &policy.model,
                    &entity_input_hash(&entity),
                    vec![1.0, 0.0, 0.0, 0.0],
                    &first,
                )
                .await
                .unwrap();
            let page = configs
                .record_embedding_reindex_page_with_attempt(
                    &staged,
                    Some(entity.base.id.to_string()),
                    1,
                    false,
                    &first,
                )
                .await
                .unwrap();
            jobs.requeue_with_delay("claim-fenced", &claim.base.id, &first.lease_token, 0, None)
                .await
                .unwrap()
                .unwrap();
            let claim = jobs
                .claim_with_lease(
                    "claim-fenced",
                    &claim.base.id,
                    "second",
                    Utc::now() + chrono::Duration::minutes(5),
                )
                .await
                .unwrap()
                .unwrap();
            let second = fence_from_claim(&claim).unwrap();
            assert!(second.generation > first.generation);
            assert!(nodes
                .store_reindex_embeddings_with_attempt(&page, vec![embedding], &first)
                .await
                .is_err());
            assert!(
                crate::reindex_guard::validate_attempt(&client, &page, &first)
                    .await
                    .is_err()
            );
            assert!(vectors
                .store_with_attempt(
                    &page,
                    &entity,
                    &policy.model,
                    &entity_input_hash(&entity),
                    vec![0.0, 1.0, 0.0, 0.0],
                    &first
                )
                .await
                .is_err());
            assert!(configs
                .record_embedding_reindex_page_with_attempt(&page, None, 0, true, &first)
                .await
                .is_err());
            assert!(configs
                .restart_embedding_reindex_with_attempt(&page, &first)
                .await
                .is_err());
            assert_eq!(
                configs
                    .get_by_space_id("claim-fenced")
                    .await
                    .unwrap()
                    .unwrap(),
                page
            );
            let entities = configs
                .record_embedding_reindex_page_with_attempt(&page, None, 0, true, &second)
                .await
                .unwrap();
            let ready = configs
                .record_embedding_reindex_page_with_attempt(&entities, None, 0, true, &second)
                .await
                .unwrap();
            assert!(
                ready
                    .processing
                    .as_ref()
                    .unwrap()
                    .pending_reindex
                    .as_ref()
                    .unwrap()
                    .complete
            );
            assert_eq!(
                ready
                    .processing
                    .as_ref()
                    .unwrap()
                    .active_embedding_profile_id
                    .as_deref(),
                Some("v1")
            );
            let cancelled = configs.cancel_embedding_reindex(&ready).await.unwrap();
            let replacement = configs
                .start_embedding_reindex(&cancelled, "v2")
                .await
                .unwrap();
            // The same valid worker lease is still unauthorized for a new target.
            assert!(
                crate::reindex_guard::validate_attempt(&client, &replacement, &second)
                    .await
                    .is_err()
            );
            assert!(configs
                .record_embedding_reindex_page_with_attempt(&replacement, None, 0, true, &second)
                .await
                .is_err());
            // A differently typed work item cannot grant reindex publication authority.
            work.base.id = RecordId::new_v4(TableName::WorkItem);
            work.kind = "embed_document".into();
            work.idempotency_key = uuid::Uuid::new_v4().to_string();
            work.payload = serde_json::json!({"reindex_id": replacement.processing.as_ref().unwrap().pending_reindex.as_ref().unwrap().id});
            let work = jobs.create(work).await.unwrap();
            let wrong = jobs
                .claim_with_lease(
                    "claim-fenced",
                    &work.base.id,
                    "wrong-kind",
                    Utc::now() + chrono::Duration::minutes(5),
                )
                .await
                .unwrap()
                .unwrap();
            assert!(crate::reindex_guard::validate_attempt(
                &client,
                &replacement,
                &fence_from_claim(&wrong).unwrap()
            )
            .await
            .is_err());
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_in_scrutinee,
        reason = "Construct both backend fixtures before the loop and retain their original teardown order."
    )]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep configs, entities in the original fixture scope through assertions and cleanup."
    )]
    async fn populated_initial_authority_requires_bootstrap_reindex_on_both_backends() {
        use elephant_types::provider_binding::ModelOperation;
        use elephant_types::records::{base::BaseFields, entity::EntityType, Entity};
        for client in [
            StorageClient::new_memory().await.unwrap(),
            StorageClient::new_embedded().await.unwrap(),
        ] {
            if client.embedded_store().is_none() {
                client
                    .query_bind_checked(
                        include_str!("../../migrations/sql/033_space_processing.surql"),
                        crate::bindings! {},
                    )
                    .await
                    .unwrap();
            }
            let configs = SpaceConfigRepository::new(client.clone());
            let entities = crate::EntityRepository::new(client);
            let original = configs
                .create(create_test_space_config("bootstrap-existing"))
                .await
                .unwrap();
            entities
                .create(Entity::new(
                    BaseFields::new(
                        RecordId::new_v4(TableName::Entity),
                        "bootstrap-existing".into(),
                        "test".into(),
                    ),
                    EntityType::new("person"),
                    "Existing entity".into(),
                ))
                .await
                .unwrap();
            let mut proposed = approved_config("bootstrap-existing");
            proposed.base = original.base.clone();
            assert!(configs
                .replace_authorized(&original, proposed.clone())
                .await
                .is_err());
            proposed
                .processing
                .as_mut()
                .unwrap()
                .active_embedding_profile_id = None;
            let configured = configs
                .replace_authorized(&original, proposed)
                .await
                .unwrap();
            let staged = configs
                .start_embedding_reindex(&configured, "v2")
                .await
                .unwrap();
            assert!(staged
                .processing
                .as_ref()
                .unwrap()
                .pending_reindex
                .as_ref()
                .unwrap()
                .from_profile_id
                .is_none());
            assert!(configs
                .authorize_model_operation(
                    "bootstrap-existing",
                    ModelOperation::QueryEmbedding,
                    None
                )
                .await
                .is_err());
            assert_eq!(
                configs
                    .authorize_model_operation(
                        "bootstrap-existing",
                        ModelOperation::ReindexEmbedding,
                        None
                    )
                    .await
                    .unwrap()
                    .profile
                    .id,
                "v2"
            );

            entities
                .create(Entity::new(
                    BaseFields::new(
                        RecordId::new_v4(TableName::Entity),
                        "bootstrap-new".into(),
                        "test".into(),
                    ),
                    EntityType::new("person"),
                    "Pre-registry entity".into(),
                ))
                .await
                .unwrap();
            let mut proposed = approved_config("bootstrap-new");
            assert!(configs.create(proposed.clone()).await.is_err());
            proposed
                .processing
                .as_mut()
                .unwrap()
                .active_embedding_profile_id = None;
            configs.create(proposed).await.unwrap();

            let mut forged = approved_config("forged-progress");
            forged.processing.as_mut().unwrap().index_generation = 42;
            assert!(configs.create(forged).await.is_err());
        }
    }
}

#[cfg(test)]
mod local_model_owner_tests {
    use super::*;

    #[tokio::test]
    async fn fresh_repositories_share_client_model_owner_without_global_retention(
    ) -> StorageResult<()> {
        let first = StorageClient::new(crate::StorageConfig {
            backend: crate::StorageBackendKind::ElephantDb,
            ..crate::StorageConfig::default()
        })
        .await?;
        let independent = StorageClient::new(crate::StorageConfig {
            backend: crate::StorageBackendKind::ElephantDb,
            ..crate::StorageConfig::default()
        })
        .await?;
        let a = SpaceConfigRepository::new(first.clone()).local_models();
        let b = SpaceConfigRepository::new(first).local_models();
        let other = SpaceConfigRepository::new(independent).local_models();
        assert!(std::sync::Arc::ptr_eq(&a, &b));
        assert!(!std::sync::Arc::ptr_eq(&a, &other));
        Ok(())
    }
}
