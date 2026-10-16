//! Entity merge repository implementation for `SurrealDB`.

#![allow(clippy::items_after_statements)]

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;

use elephant_types::records::{EntityMerge, MergeStrategy};

use elephant_types::{Link, RecordId, TableName};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::{PaginatedResult, Repository, SearchCriteria, SearchRepository};
use crate::require::required_field;

pub(crate) const ENTITY_MERGE_PROJECTION: &str = r"
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
    `from`,
    `into`,
    strategy,
    decided_by,
    decided_at,
    run_id
";

/// Database-only proxy for `EntityMerge` to avoid flatten/native enum issues.
#[derive(Deserialize)]
pub(crate) struct EntityMergeDb {
    id: surrealdb::sql::Thing,
    space_id: Option<String>,
    created_at: Option<chrono::DateTime<Utc>>,
    updated_at: Option<chrono::DateTime<Utc>>,
    created_by: Option<String>,
    deleted_at: Option<chrono::DateTime<Utc>>,
    provenance: Option<elephant_types::records::base::Provenance>,
    security: Option<elephant_types::records::base::Security>,
    subjects: Option<Vec<surrealdb::sql::Thing>>,
    subjects_state: Option<elephant_types::records::base::SubjectsState>,
    from: Option<surrealdb::sql::Thing>,
    into: Option<surrealdb::sql::Thing>,
    strategy: Option<MergeStrategy>,
    decided_by: Option<String>,
    decided_at: Option<chrono::DateTime<Utc>>,
    run_id: Option<String>,
}

impl TryFrom<EntityMergeDb> for EntityMerge {
    type Error = StorageError;

    fn try_from(db: EntityMergeDb) -> Result<Self, Self::Error> {
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
                subjects: required_field(db.subjects, "subjects")?
                    .into_iter()
                    .map(|t| crate::surreal::record_id_from_thing(&t))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| StorageError::QueryError(e.to_string()))?
                    .into_iter()
                    .map(Link::new)
                    .collect(),
                subjects_state: required_field(db.subjects_state, "subjects_state")?,
            },
            from: Link::new(
                crate::surreal::record_id_from_thing(
                    &db.from
                        .ok_or_else(|| StorageError::QueryError("Missing from link".to_string()))?,
                )
                .map_err(|e| StorageError::QueryError(e.to_string()))?,
            ),
            into: Link::new(
                crate::surreal::record_id_from_thing(
                    &db.into
                        .ok_or_else(|| StorageError::QueryError("Missing into link".to_string()))?,
                )
                .map_err(|e| StorageError::QueryError(e.to_string()))?,
            ),
            strategy: required_field(db.strategy, "strategy")?,
            decided_by: required_field(db.decided_by, "decided_by")?,
            decided_at: db
                .decided_at
                .ok_or_else(|| StorageError::QueryError("Missing decided_at".to_string()))?,
            run_id: db.run_id,
        })
    }
}

/// Entity merge repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct EntityMergeRepository {
    client: StorageClient,
}

impl EntityMergeRepository {
    /// Creates a new entity merge repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl Repository<EntityMerge> for EntityMergeRepository {
    async fn create(&self, record: EntityMerge) -> StorageResult<EntityMerge> {
        if let Some(store) = self.client.embedded_store() {
            return store.create(record).await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!("CREATE $id CONTENT $record RETURN {ENTITY_MERGE_PROJECTION};"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => crate::surreal::entity_merge_to_value(&record)?,
                },
            )
            .await?;

        let merges: Vec<EntityMergeDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created entity_merge: {e}"))
        })?;

        let merge_db = merges.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but entity_merge not found".to_string())
        })?;

        EntityMerge::try_from(merge_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<EntityMerge>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<EntityMerge>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {ENTITY_MERGE_PROJECTION} FROM entity_merge WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let merges: Vec<EntityMergeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse entity_merge: {e}")))?;

        merges
            .into_iter()
            .next()
            .map(EntityMerge::try_from)
            .transpose()
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: EntityMerge,
    ) -> StorageResult<EntityMerge> {
        let mut normalized = record;
        normalized.base.space_id = space_id.to_string();
        normalized.base.id.clone_from(id);

        if let Some(store) = self.client.embedded_store() {
            return store.update_existing(space_id, id, normalized).await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {ENTITY_MERGE_PROJECTION};"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => crate::surreal::entity_merge_to_value(&normalized)?,
                },
            )
            .await?;

        let merges: Vec<EntityMergeDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated entity_merge: {e}"))
        })?;

        let merge_db = merges.into_iter().next().ok_or_else(|| {
            StorageError::NotFound(format!("Entity merge not found in space: {id}"))
        })?;

        EntityMerge::try_from(merge_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<EntityMerge>(space_id, id).await;
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
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<EntityMerge>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<EntityMerge>(space_id, limit, cursor, false)
                .await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {ENTITY_MERGE_PROJECTION} FROM entity_merge WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {ENTITY_MERGE_PROJECTION} FROM entity_merge WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
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
                    "space_id" => space_id
                },
            )
            .await?;

        let merges: Vec<EntityMergeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse entity_merges: {e}")))?;

        merges.into_iter().map(EntityMerge::try_from).collect()
    }
}

#[async_trait]
impl SearchRepository<EntityMerge> for EntityMergeRepository {
    async fn search(
        &self,
        criteria: &SearchCriteria,
    ) -> StorageResult<PaginatedResult<EntityMerge>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .search::<EntityMerge, _>(criteria, |_merge, _criteria| true)
                .await;
        }

        #[derive(serde::Deserialize)]
        struct CountResult {
            total: usize,
        }

        let mut conditions = Vec::new();
        let mut bindings = std::collections::BTreeMap::new();

        if !criteria.include_deleted {
            conditions.push("deleted_at IS NONE".to_string());
        }

        if let Some(ref space_id) = criteria.space_id {
            conditions.push("space_id = $space_id".to_string());
            bindings.insert(
                "space_id".to_string(),
                surrealdb::sql::Value::from(space_id.clone()),
            );
        }

        if let Some(cursor) = criteria.cursor.as_ref() {
            let rid = crate::parse_id(cursor, TableName::EntityMerge)
                .map_err(|e| StorageError::QueryError(e.to_string()))?;
            conditions.push("id > $cursor".to_string());
            bindings.insert(
                "cursor".to_string(),
                surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(&rid)),
            );
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let query = format!(
            "SELECT {ENTITY_MERGE_PROJECTION} FROM entity_merge {where_clause} ORDER BY id LIMIT {}",
            criteria.limit + 1
        );

        let mut response = self.client.query_bind(&query, bindings.clone()).await?;

        let merges_db: Vec<EntityMergeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse entity_merges: {e}")))?;

        let mut merges: Vec<EntityMerge> = merges_db
            .into_iter()
            .map(EntityMerge::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        let has_more = merges.len() > criteria.limit;
        if has_more {
            merges.pop();
        }

        let next_cursor = if has_more {
            merges.last().map(|m| {
                m.base
                    .id
                    .to_string()
                    .split(':')
                    .nth(1)
                    .unwrap_or(&m.base.id.to_string())
                    .to_string()
            })
        } else {
            None
        };

        let count_query =
            format!("SELECT count() as total FROM entity_merge {where_clause} GROUP ALL");

        let mut count_response = self.client.query_bind(&count_query, bindings).await?;

        let counts: Vec<CountResult> =
            crate::response::take_first(&mut count_response, "Failed to parse entity merge count")?;
        let total = counts.first().map_or(0, |c| c.total);

        Ok(PaginatedResult::new(merges, next_cursor, has_more, total))
    }
}

impl EntityMergeRepository {
    /// Reads physical row state for atomic source lifecycle inverse preparation.
    pub(crate) async fn get_including_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<EntityMerge>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<EntityMerge>(space_id, id, true).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {ENTITY_MERGE_PROJECTION} FROM entity_merge WHERE id = $id AND space_id = $space_id"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let merges: Vec<EntityMergeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse entity_merge: {e}")))?;

        merges
            .into_iter()
            .next()
            .map(EntityMerge::try_from)
            .transpose()
    }
}
// Explicit equivalence is a derived canonical decision. All reads precede
// authorization and all writes, including reference rewrites, share one commit.
use crate::canonical_commit::{
    CanonicalCommit, CanonicalMaintenanceContext, CanonicalMaintenanceOrigin, CanonicalPlan,
    CanonicalRecord, CanonicalRepository,
};
use crate::canonical_extension::CanonicalExtensionEffect;
use elephant_types::records::base::{Security, SubjectsState};
use elephant_types::records::{AuditAction, AuditLog, Entity, HasSecurity, IdMap, SystemEvent};
use serde::Serialize;

/// Existing transport strategies, resolved from the same observed entity rows.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeSelection {
    /// Preserve A's identity.
    KeepA,
    /// Preserve B's identity.
    KeepB,
    /// Preserve the older entity's identity.
    PreferOlder,
    /// Preserve the more recently updated entity's identity.
    PreferNewer,
    /// Create one deterministic new winner and redirect both inputs.
    Combine,
}

/// Committed result of an explicit identity equivalence decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityMergeOutcome {
    /// Complete proposed winner, including joined restrictions.
    pub survivor: Entity,
    /// Redirected input identities.
    pub merged: Vec<RecordId>,
    /// Number of changed relationship rows, without double counting self edges.
    pub relationships_repointed: usize,
    /// Number of changed domain/system event rows.
    pub events_repointed: usize,
    /// Durable decision lineage rows committed atomically.
    pub merge_record_ids: Vec<RecordId>,
}

/// Complete authorization envelope for one observed or proposed merge row.
#[derive(Debug, Clone)]
pub struct MergeAuthorization {
    /// Record identity for diagnostics, never raw row payload.
    pub id: String,
    /// Scope required to observe this input/output.
    pub read_scope: &'static str,
    /// Scope required for the declared mutation.
    pub write_scope: &'static str,
    space_id: String,
    security: Security,
    subjects: Vec<Link<Entity>>,
    subjects_state: SubjectsState,
}

impl HasSecurity for MergeAuthorization {
    fn space_id(&self) -> &str {
        &self.space_id
    }
    fn security(&self) -> &Security {
        &self.security
    }
    fn subjects(&self) -> Vec<Link<Entity>> {
        self.subjects.clone()
    }
    fn subjects_state(&self) -> SubjectsState {
        self.subjects_state
    }
}

/// Read-only prepared bundle. Adapters must authorize every envelope returned
/// by `authorizations` before invoking `commit`; no mutation occurs in prepare.
pub struct PreparedEntityMerge {
    client: StorageClient,
    request: CanonicalCommit,
    outcome: EntityMergeOutcome,
    prior_plan: Option<CanonicalPlan>,
    actor: String,
    now: chrono::DateTime<Utc>,
    #[cfg(test)]
    fault: Option<crate::canonical_commit::CanonicalFault>,
}

impl EntityMergeRepository {
    /// Prepare one explicit equivalence under a predicate fence captured before
    /// the first entity or dependent-record read.
    ///
    /// Extension epoch guards must
    /// already be installed by the registry-aware identity adapter.
    pub async fn prepare_merge(
        &self,
        space: &str,
        entity_a: &RecordId,
        entity_b: &RecordId,
        selection: MergeSelection,
        actor: &str,
    ) -> StorageResult<PreparedEntityMerge> {
        self.prepare_merge_group(
            space,
            entity_a,
            std::slice::from_ref(entity_b),
            selection,
            actor,
        )
        .await
    }

    /// Prepare the retained trusted multi-identity merge as one complete bundle.
    pub async fn prepare_merge_group(
        &self,
        space: &str,
        entity_a: &RecordId,
        others: &[RecordId],
        selection: MergeSelection,
        actor: &str,
    ) -> StorageResult<PreparedEntityMerge> {
        self.prepare_merge_group_on_plan(space, entity_a, others, selection, actor, None)
            .await
    }

    /// Prepare using a caller's original epoch and pending typed record overlay.
    ///
    /// A supplied plan is returned only through `into_prepared_plan`; it cannot
    /// be committed as an independent merge or acquire a new attempt authority.
    #[allow(
        clippy::too_many_lines,
        reason = "Keep the original epoch, prior-plan overlay and complete identity-reference effects together in one prepared merge."
    )]
    pub async fn prepare_merge_group_on_plan(
        &self,
        space: &str,
        entity_a: &RecordId,
        others: &[RecordId],
        selection: MergeSelection,
        actor: &str,
        prior_plan: Option<&CanonicalPlan>,
    ) -> StorageResult<PreparedEntityMerge> {
        use elephant_types::records::entity::EntityStatus;
        let mut ids = std::collections::BTreeSet::new();
        if space.trim().is_empty()
            || actor.trim().is_empty()
            || others.is_empty()
            || std::iter::once(entity_a)
                .chain(others)
                .any(|id| id.table() != TableName::Entity || !ids.insert(id.clone()))
        {
            return Err(merge_conflict(
                "merge requires distinct entity identities in one selected space",
            ));
        }
        let canonical = CanonicalRepository::new(self.client.clone());
        let current_epoch = canonical.epoch(space).await?;
        let epoch = if let Some(prior) = prior_plan {
            if prior.observed_epoch != current_epoch {
                return Err(merge_conflict(
                    "maintenance epoch changed before merge preparation",
                ));
            }
            prior.observed_epoch.clone()
        } else {
            current_epoch
        };
        let mut inputs = Vec::new();
        let mut dependencies = Vec::new();
        for id in std::iter::once(entity_a).chain(others) {
            let live = CanonicalRecord::load(&self.client, space, id).await?;
            if let Some(row) = &live {
                dependencies.push(row.clone());
            }
            let Some(CanonicalRecord::Entity(row)) = merge_overlay_record(prior_plan, id, live)?
            else {
                return Err(StorageError::NotFound(id.to_string()));
            };
            let row = *row;
            if row.base.space_id != space
                || row.base.deleted_at.is_some()
                || row.status != EntityStatus::Active
                || row.redirect_to.is_some()
            {
                return Err(merge_conflict(
                    "explicit merge inputs must be current active identities",
                ));
            }
            inputs.push(row);
        }
        let a = &inputs[0];
        let b = &inputs[1];
        let combine = matches!(selection, MergeSelection::Combine);
        let prefer_b = matches!(selection, MergeSelection::KeepB)
            || matches!(selection, MergeSelection::PreferOlder)
                && b.base.created_at < a.base.created_at
            || matches!(selection, MergeSelection::PreferNewer)
                && b.base.updated_at > a.base.updated_at;
        let winner = if prefer_b { b } else { a };
        // Merge intent is not a stored row: retain every input field rather
        // than invoking the source-row fingerprint's timestamp normalization.
        let intent = serde_json::to_vec(&serde_json::json!({
            "operation": "identity_merge", "space_id": space, "actor": actor, "selection": selection, "inputs": &inputs,
        }))
        .map_err(merge_encoding)?;
        let fingerprint = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, &intent).to_string();
        let commit_id = format!("identity-merge:{fingerprint}");
        let now = Utc::now();
        let mut survivor = winner.clone();
        for other in inputs
            .iter()
            .filter(|input| input.base.id != winner.base.id)
        {
            survivor = merge_entity_payloads(&survivor, other, combine)?;
        }
        if combine {
            survivor.base.id = merge_id(TableName::Entity, &commit_id, "combined");
            survivor.base.created_at = now;
            survivor.base.created_by = actor.into();
        }
        survivor.base.updated_at = now;
        let merged: Vec<_> = inputs
            .iter()
            .filter(|input| combine || input.base.id != winner.base.id)
            .cloned()
            .collect();
        let merged_ids: Vec<_> = merged.iter().map(|row| row.base.id.clone()).collect();
        let mut plan = CanonicalPlan {
            observed_epoch: epoch,
            dependencies,
            ..CanonicalPlan::default()
        };
        plan.push(
            if combine {
                None
            } else {
                Some(CanonicalRecord::Entity(Box::new(winner.clone())))
            },
            Some(CanonicalRecord::Entity(Box::new(survivor.clone()))),
        )?;
        for row in &merged {
            let mut redirect = row.clone();
            redirect.status = EntityStatus::Merged;
            redirect.redirect_to = Some(Link::new(survivor.base.id.clone()));
            redirect.base.updated_at = now;
            crate::public_mutation::inherit_public_envelope(&mut redirect.base, &survivor.base)?;
            plan.push(
                Some(CanonicalRecord::Entity(Box::new(row.clone()))),
                Some(CanonicalRecord::Entity(Box::new(redirect))),
            )?;
        }
        let mut relationship_count = 0;
        let mut event_count = 0;
        // Historical rows keep their lifecycle; a merge only changes declared
        // identity references. Their exact old/new envelopes are still authorized.
        for table in [
            TableName::EntityIdentityKey,
            TableName::Rel,
            TableName::Attribute,
            TableName::Assertion,
            TableName::Doc,
            TableName::Event,
            TableName::SystemEvent,
            TableName::IdentityCandidate,
            TableName::IdMap,
        ] {
            let rows = merge_scan_on_plan(&self.client, space, table, prior_plan).await?;
            for before in rows {
                let mut after = before.clone();
                if !rewrite_core_identity(&mut after, &merged_ids, &survivor.base.id) {
                    continue;
                }
                inherit_identity_reference_restrictions(after.base_mut(), &survivor.base)?;
                after.base_mut().updated_at = now;
                if table == TableName::Rel {
                    relationship_count += 1;
                }
                if matches!(table, TableName::Event | TableName::SystemEvent) {
                    event_count += 1;
                }
                plan.push(Some(before), Some(after))?;
            }
        }
        let mut merge_record_ids = Vec::new();
        for row in &merged {
            let mut base = survivor.base.clone();
            base.id = merge_id(TableName::EntityMerge, &commit_id, &row.base.id.to_string());
            base.created_by = actor.into();
            base.created_at = now;
            base.updated_at = now;
            merge_record_ids.push(base.id.clone());
            plan.push(
                None,
                Some(CanonicalRecord::EntityMerge(Box::new(EntityMerge {
                    base: base.clone(),
                    from: Link::new(row.base.id.clone()),
                    into: Link::new(survivor.base.id.clone()),
                    strategy: if combine {
                        MergeStrategy::Combine
                    } else if prefer_b {
                        MergeStrategy::KeepB
                    } else {
                        MergeStrategy::KeepA
                    },
                    decided_by: actor.into(),
                    decided_at: now,
                    run_id: Some(commit_id.clone()),
                }))),
            )?;
            base.id = merge_id(TableName::IdMap, &commit_id, &row.base.id.to_string());
            plan.push(
                None,
                Some(CanonicalRecord::IdMap(Box::new(IdMap {
                    base,
                    legacy_id: row.base.id.to_string(),
                    record: survivor.base.id.clone(),
                    source_system: "explicit_identity_merge".into(),
                }))),
            )?;
        }
        Ok(PreparedEntityMerge {
            client: self.client.clone(),
            #[cfg(test)]
            fault: None,
            actor: actor.into(),
            now,
            prior_plan: prior_plan.cloned(),
            outcome: EntityMergeOutcome {
                survivor,
                merged: merged_ids,
                relationships_repointed: relationship_count,
                events_repointed: event_count,
                merge_record_ids,
            },
            request: CanonicalCommit {
                space_id: space.into(),
                commit_id,
                input_fingerprint: fingerprint,
                replay_output: None,
                publication: None,
                maintenance: None,
                finalization: None,
                plan,
            },
        })
    }
}

impl PreparedEntityMerge {
    /// Stable winner and redirected identities used by declared extension refs.
    #[must_use]
    pub const fn outcome(&self) -> &EntityMergeOutcome {
        &self.outcome
    }

    /// Include extension effects prepared under this bundle's original epoch.
    pub async fn attach_extensions(
        &mut self,
        effects: Vec<CanonicalExtensionEffect>,
    ) -> StorageResult<()> {
        for effect in &effects {
            crate::canonical_extension::validate_extension_effect(&self.request.space_id, effect)?;
            // Force complete envelope validation now, before transport approval.
            for row in [&effect.before, &effect.after].into_iter().flatten() {
                extension_authorization(&effect.id, row)?;
            }
            if let Some(surrealdb::sql::Value::Object(row)) = &effect.before {
                if let Some(surrealdb::sql::Value::Object(provenance)) = row.get("provenance") {
                    if let Some(surrealdb::sql::Value::Array(sources)) = provenance.get("sources") {
                        for source in sources.iter() {
                            let source = match source {
                                surrealdb::sql::Value::Thing(id) => {
                                    crate::surreal::record_id_from_thing(id)?
                                }
                                surrealdb::sql::Value::Strand(id) => {
                                    crate::parse_id(&id.0, TableName::Source)?
                                }
                                _ => return Err(merge_conflict(
                                    "extension source provenance must contain source identities",
                                )),
                            };
                            let observed = CanonicalRecord::load(
                                &self.client,
                                &self.request.space_id,
                                &source,
                            )
                            .await?
                            .ok_or_else(|| {
                                merge_conflict("extension source provenance is unavailable")
                            })?;
                            self.request.plan.dependencies.push(observed);
                        }
                    }
                }
            }
        }
        self.request.plan.extension_effects = effects;
        Ok(())
    }

    /// Every old and proposed row must pass both read and write policy checks.
    pub fn authorizations(&self) -> StorageResult<Vec<MergeAuthorization>> {
        let mut records = Vec::new();
        for record in self.request.plan.dependencies.iter().chain(
            self.request
                .plan
                .effects
                .iter()
                .flat_map(|effect| [&effect.before, &effect.after].into_iter().flatten()),
        ) {
            records.push(core_authorization(record));
        }
        for effect in &self.request.plan.extension_effects {
            for row in [&effect.before, &effect.after].into_iter().flatten() {
                records.push(extension_authorization(&effect.id, row)?);
            }
        }
        Ok(records)
    }

    /// Consume a merge into the caller's complete plan without live writes.
    ///
    /// Prior effects keep their original before images and epoch; merge audit
    /// and propagation are included in the one later canonical commit.
    pub fn into_prepared_plan(mut self) -> StorageResult<CanonicalPlan> {
        self.prepare_propagation()?;
        let Some(mut plan) = self.prior_plan.take() else {
            return Ok(self.request.plan);
        };
        for dependency in self.request.plan.dependencies {
            if let Some(existing) = plan
                .dependencies
                .iter()
                .find(|row| row.base().id == dependency.base().id)
            {
                if !same_merge_record(existing, &dependency)? {
                    return Err(merge_conflict(
                        "merge dependency changed from original plan",
                    ));
                }
            } else {
                plan.dependencies.push(dependency);
            }
        }
        for effect in self.request.plan.effects {
            plan.push(effect.before, effect.after)?;
        }
        for effect in self.request.plan.extension_effects {
            if let Some(existing) = plan
                .extension_effects
                .iter_mut()
                .find(|row| row.id == effect.id)
            {
                if existing.after != effect.before {
                    return Err(merge_conflict("nonsequential extension merge effects"));
                }
                existing.after = effect.after;
            } else {
                plan.extension_effects.push(effect);
            }
        }
        plan.outbox.extend(self.request.plan.outbox);
        Ok(plan)
    }

    fn prepare_propagation(&mut self) -> StorageResult<()> {
        // Propagation and per-row audit are effects of this same transaction.
        // A allocates the outbox sequence; no post-success warning-only writes.
        let mut event_base = self.outcome.survivor.base.clone();
        let mut changed = Vec::new();
        for effect in self.request.plan.effects.clone() {
            let Some(after) = &effect.after else {
                continue;
            };
            crate::public_mutation::inherit_public_envelope(&mut event_base, after.base())?;
            let mut base = after.base().clone();
            base.id = merge_id(
                TableName::AuditLog,
                &self.request.commit_id,
                &after.base().id.to_string(),
            );
            base.created_at = self.now;
            base.updated_at = self.now;
            base.deleted_at = None;
            base.created_by.clone_from(&self.actor);
            let before = effect.before.as_ref().map(canonical_json).transpose()?;
            let after_value = canonical_json(after)?;
            let mut audit = AuditLog {
                base,
                action: if effect.before.is_some() {
                    AuditAction::Update
                } else {
                    AuditAction::Create
                },
                actor: self.actor.clone(),
                record: after.base().id.clone(),
                before,
                after: Some(after_value),
                timestamp: self.now,
                reason: Some(self.request.commit_id.clone()),
            };
            // A rewritten subject can leave the live row, but remains in the
            // audit's before image. Retain both trusted target envelopes.
            crate::canonical_commit::inherit_maintenance_audit_authority(
                &self.request.plan,
                &mut audit,
            )?;
            self.request
                .plan
                .push(None, Some(CanonicalRecord::AuditLog(Box::new(audit))))?;
            changed.push(serde_json::json!({"id": after.base().id, "table": after.base().id.table().as_str(), "op": if effect.before.is_some() {"update"} else {"create"}}));
        }
        for envelope in self.authorizations()? {
            let mut base = event_base.clone();
            base.security = envelope.security;
            base.subjects = envelope.subjects;
            base.subjects_state = envelope.subjects_state;
            crate::public_mutation::inherit_public_envelope(&mut event_base, &base)?;
        }
        event_base.id = merge_id(TableName::SystemEvent, &self.request.commit_id, "event");
        event_base.created_by.clone_from(&self.actor);
        event_base.created_at = self.now;
        event_base.updated_at = self.now;
        event_base.deleted_at = None;
        let extension_records: Vec<_> = self.request.plan.extension_effects.iter().map(|effect|
            serde_json::json!({"id": effect.id.to_string(), "table":effect.id.tb, "op":"update"})).collect();
        self.request.plan.outbox.push(SystemEvent {
            base: event_base, seq: 0, event_type: "entity.merged".into(),
            payload: serde_json::json!({"space_id":self.request.space_id,"event_type":"entity.merged","actor":self.actor,"survivor":self.outcome.survivor.base.id,"merged":self.outcome.merged,"records":changed,"extension_records":extension_records}),
        });
        Ok(())
    }

    /// Commit the fully authorized bundle with its original epoch and observed
    /// support conjunction. No latest-head reconstruction or invented attempt.
    pub async fn commit(mut self) -> StorageResult<EntityMergeOutcome> {
        if self.prior_plan.is_some() {
            return Err(merge_conflict(
                "prepared maintenance merge must join its caller's original plan",
            ));
        }
        let canonical = CanonicalRepository::new(self.client.clone());
        let mut supporting = self.request.plan.dependencies.clone();
        supporting.extend(
            self.request
                .plan
                .effects
                .iter()
                .filter_map(|effect| effect.before.clone()),
        );
        let sources = canonical
            .capture_maintenance_support_with_extensions(
                &self.request.space_id,
                &supporting,
                &self.request.plan.extension_effects,
            )
            .await?;
        self.request.maintenance = Some(CanonicalMaintenanceContext {
            origin: CanonicalMaintenanceOrigin::Derived,
            attempt: None,
            sources,
        });
        self.prepare_propagation()?;
        self.request.replay_output =
            Some(serde_json::to_value(&self.outcome).map_err(merge_encoding)?);
        #[cfg(test)]
        let receipt = match self.fault {
            Some(fault) => Box::pin(canonical.commit_with_fault(self.request, fault)).await?,
            None => Box::pin(canonical.commit(self.request)).await?,
        };
        #[cfg(not(test))]
        let receipt = Box::pin(canonical.commit(self.request)).await?;
        drop(canonical);
        serde_json::from_value(
            receipt
                .replay_output
                .ok_or_else(|| merge_conflict("merge receipt has no committed result"))?,
        )
        .map_err(merge_encoding)
    }
}

fn canonical_json(record: &CanonicalRecord) -> StorageResult<serde_json::Value> {
    // CanonicalRecord is a tagged enum; audit stores its domain record payload.
    let value = serde_json::to_value(record).map_err(merge_encoding)?;
    value
        .get("record")
        .cloned()
        .ok_or_else(|| merge_conflict("canonical audit record shape is unsupported"))
}

fn same_merge_record(a: &CanonicalRecord, b: &CanonicalRecord) -> StorageResult<bool> {
    Ok(serde_json::to_value(a).map_err(merge_encoding)?
        == serde_json::to_value(b).map_err(merge_encoding)?)
}

fn merge_overlay_record(
    prior: Option<&CanonicalPlan>,
    id: &RecordId,
    live: Option<CanonicalRecord>,
) -> StorageResult<Option<CanonicalRecord>> {
    if let Some(effect) = prior.and_then(|plan| {
        plan.effects.iter().find(|effect| {
            effect
                .after
                .as_ref()
                .or(effect.before.as_ref())
                .is_some_and(|row| &row.base().id == id)
        })
    }) {
        let matches = match (&effect.before, &live) {
            (Some(before), Some(current)) => same_merge_record(before, current)?,
            (None, None) => true,
            _ => false,
        };
        if !matches {
            return Err(merge_conflict(
                "merge input changed from original plan before image",
            ));
        }
        return Ok(effect.after.clone());
    }
    Ok(live)
}

async fn merge_scan_on_plan(
    client: &StorageClient,
    space: &str,
    table: TableName,
    prior: Option<&CanonicalPlan>,
) -> StorageResult<Vec<CanonicalRecord>> {
    let mut rows: std::collections::BTreeMap<_, _> = merge_scan(client, space, table)
        .await?
        .into_iter()
        .map(|row| (row.base().id.clone(), row))
        .collect();
    if let Some(plan) = prior {
        for effect in &plan.effects {
            let Some(row) = effect.after.as_ref().or(effect.before.as_ref()) else {
                continue;
            };
            if row.base().id.table() != table {
                continue;
            }
            if row.base().space_id != space {
                return Err(merge_conflict("merge overlay crossed space"));
            }
            let id = &row.base().id;
            let current = merge_overlay_record(Some(plan), id, rows.remove(id))?;
            if let Some(current) = current {
                rows.insert(id.clone(), current);
            }
        }
    }
    if rows.len() > 100_000 {
        return Err(merge_conflict(
            "merge overlay exceeds bounded reference scan",
        ));
    }
    Ok(rows.into_values().collect())
}

pub(crate) async fn merge_scan(
    client: &StorageClient,
    space: &str,
    table: TableName,
) -> StorageResult<Vec<CanonicalRecord>> {
    const MAX_ROWS: usize = 100_000;
    let ids: Vec<RecordId> = if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| {
                Ok(tx
                    .scan_raw_rows(table)?
                    .into_iter()
                    .filter(|(_, row)| {
                        row.get("space_id").and_then(serde_json::Value::as_str) == Some(space)
                    })
                    .map(|(id, _)| id)
                    .collect())
            })
            .await?
    } else {
        #[derive(Deserialize)]
        struct IdOnly {
            id: surrealdb::sql::Thing,
        }
        let mut response = client
            .query_bind_checked(
                &format!(
                    "SELECT id FROM {} WHERE space_id = $space LIMIT {};",
                    table.as_str(),
                    MAX_ROWS + 1
                ),
                crate::bindings! { "space" => space },
            )
            .await?;
        let rows: Vec<IdOnly> = response
            .take(0)
            .map_err(|error| StorageError::QueryError(error.to_string()))?;
        rows.into_iter()
            .map(|row| crate::surreal::record_id_from_thing(&row.id).map_err(StorageError::from))
            .collect::<StorageResult<_>>()?
    };
    if ids.len() > MAX_ROWS {
        return Err(merge_conflict(
            "merge exceeds the bounded reference scan; no writes were made",
        ));
    }
    let mut rows = Vec::with_capacity(ids.len());
    for id in ids {
        rows.push(
            CanonicalRecord::load(client, space, &id)
                .await?
                .ok_or_else(|| merge_conflict("merge input disappeared while preparing"))?,
        );
    }
    Ok(rows)
}

fn rewrite_core_identity(
    row: &mut CanonicalRecord,
    merged: &[RecordId],
    survivor: &RecordId,
) -> bool {
    fn link<T>(value: &mut Link<T>, merged: &[RecordId], survivor: &RecordId) -> bool {
        if merged.contains(value.id()) {
            *value = Link::new(survivor.clone());
            true
        } else {
            false
        }
    }
    let mut changed = false;
    match row {
        CanonicalRecord::EntityIdentityKey(row) => {
            changed |= link(&mut row.entity, merged, survivor);
        }
        CanonicalRecord::Rel(row) => {
            changed |= link(&mut row.subject, merged, survivor);
            changed |= link(&mut row.object, merged, survivor);
        }
        CanonicalRecord::Attribute(row) => changed |= link(&mut row.subject, merged, survivor),
        CanonicalRecord::Assertion(row) => {
            for value in [&mut row.subject_entity, &mut row.object_entity]
                .into_iter()
                .flatten()
            {
                changed |= link(value, merged, survivor);
            }
        }
        CanonicalRecord::IdentityCandidate(row) => {
            changed |= link(&mut row.entity_a, merged, survivor);
            changed |= link(&mut row.entity_b, merged, survivor);
        }
        CanonicalRecord::IdMap(row) => {
            if merged.contains(&row.record) {
                row.record = survivor.clone();
                changed = true;
            }
        }
        _ => {}
    }
    for subject in &mut row.base_mut().subjects {
        changed |= link(subject, merged, survivor);
    }
    let mut seen = std::collections::HashSet::new();
    row.base_mut()
        .subjects
        .retain(|subject| seen.insert(subject.id().clone()));
    changed
}

/// Changing an identity reference does not add evidence to an existing fact.
///
/// Preserve its physical source lineage while inheriting all restrictions of
/// the merged identity. The survivor and new merge decisions separately retain
/// the contributions from every merged input.
fn inherit_identity_reference_restrictions(
    output: &mut elephant_types::records::base::BaseFields,
    input: &elephant_types::records::base::BaseFields,
) -> StorageResult<()> {
    let mut restrictions = input.clone();
    restrictions.provenance.sources.clear();
    crate::public_mutation::inherit_public_envelope(output, &restrictions)
}

fn merge_entity_payloads(winner: &Entity, other: &Entity, combine: bool) -> StorageResult<Entity> {
    let mut merged = winner.clone();
    crate::public_mutation::inherit_public_envelope(&mut merged.base, &other.base)?;
    for alias in other
        .aliases
        .iter()
        .chain(std::iter::once(&other.display_name))
    {
        merged.add_alias(alias.clone());
    }
    merged
        .identity_keys
        .extend(other.identity_keys.iter().cloned());
    merged.identity_keys.sort();
    merged.identity_keys.dedup();
    merged.metadata = merge_metadata(&winner.metadata, &other.metadata);
    for handle in &other.handles {
        match merged.handles.iter_mut().find(|existing| {
            existing.platform == handle.platform && existing.username == handle.username
        }) {
            None => merged.handles.push(handle.clone()),
            Some(existing) => {
                if handle.confidence.unwrap_or(0.0) > existing.confidence.unwrap_or(0.0) {
                    existing.confidence = handle.confidence;
                    existing.verified = handle.verified;
                }
                existing.first_seen = match (existing.first_seen, handle.first_seen) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                };
                existing.last_seen = match (existing.last_seen, handle.last_seen) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    (a, b) => a.or(b),
                };
                existing.evidence.extend(handle.evidence.iter().cloned());
                let mut seen = std::collections::HashSet::new();
                existing.evidence.retain(|id| seen.insert(id.clone()));
            }
        }
    }
    if combine {
        merged.risk_level = match (winner.risk_level, other.risk_level) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        merged.trust_level = match (winner.trust_level, other.trust_level) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        merged.known_safe |= other.known_safe;
    }
    Ok(merged)
}

fn merge_metadata(a: &serde_json::Value, b: &serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    if a == b {
        return a.clone();
    }
    match (a, b) {
        (Value::Object(left), Value::Object(right)) => {
            let mut output = left.clone();
            for (key, value) in right {
                output.insert(
                    key.clone(),
                    output
                        .get(key)
                        .map_or_else(|| value.clone(), |old| merge_metadata(old, value)),
                );
            }
            Value::Object(output)
        }
        (Value::Array(left), Value::Array(right)) => {
            let mut all = left.clone();
            all.extend(right.iter().cloned());
            all.sort_by_key(Value::to_string);
            all.dedup();
            Value::Array(all)
        }
        _ => {
            let mut all = vec![a.clone(), b.clone()];
            all.sort_by_key(Value::to_string);
            Value::Array(all)
        }
    }
}

fn core_authorization(record: &CanonicalRecord) -> MergeAuthorization {
    let base = record.base();
    let (read_scope, write_scope) = match record {
        CanonicalRecord::Rel(_) | CanonicalRecord::Attribute(_) => {
            ("read:relationship", "write:relationship")
        }
        CanonicalRecord::Assertion(_) => ("read:assertion", "write:assertion"),
        CanonicalRecord::Doc(_) => ("read:doc:metadata", "write:doc"),
        CanonicalRecord::Source(_) => ("read:source", "write:source"),
        CanonicalRecord::Event(_) | CanonicalRecord::SystemEvent(_) => {
            ("read:event", "write:event")
        }
        _ => ("read:entity", "write:entity"),
    };
    let mut subjects = base.subjects.clone();
    let mut state = base.subjects_state;
    match record {
        CanonicalRecord::Entity(row) => {
            subjects = row.subjects();
            state = row.subjects_state();
        }
        CanonicalRecord::Rel(row) => {
            subjects = row.subjects();
            state = row.subjects_state();
        }
        CanonicalRecord::Attribute(row) => {
            subjects = row.subjects();
            state = row.subjects_state();
        }
        CanonicalRecord::Assertion(row) => {
            subjects = row.subjects();
            state = row.subjects_state();
        }
        CanonicalRecord::EntityIdentityKey(row) => subjects.push(row.entity.clone()),
        CanonicalRecord::IdentityCandidate(row) => {
            subjects = row.subjects();
            state = row.subjects_state();
        }
        CanonicalRecord::EntityMerge(row) => {
            subjects = row.subjects();
            state = row.subjects_state();
        }
        _ => {}
    }
    MergeAuthorization {
        id: base.id.to_string(),
        read_scope,
        write_scope,
        space_id: base.space_id.clone(),
        security: base.security.clone(),
        subjects,
        subjects_state: state,
    }
}

fn extension_authorization(
    id: &surrealdb::sql::Thing,
    value: &surrealdb::sql::Value,
) -> StorageResult<MergeAuthorization> {
    // Clean storage proxy: mandatory fields; no flatten or JSON round trip.
    #[derive(Deserialize)]
    struct Envelope {
        space_id: String,
        security: Security,
        subjects: Vec<Link<Entity>>,
        subjects_state: SubjectsState,
    }
    let row: Envelope = surrealdb::sql::from_value(value.clone()).map_err(|error| {
        StorageError::DataIntegrityError(format!("extension authorization envelope: {error}"))
    })?;
    Ok(MergeAuthorization {
        id: id.to_string(),
        read_scope: "read:entity",
        write_scope: "write:entity",
        space_id: row.space_id,
        security: row.security,
        subjects: row.subjects,
        subjects_state: row.subjects_state,
    })
}

fn merge_id(table: TableName, commit: &str, role: &str) -> RecordId {
    RecordId::new(
        table,
        uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            format!("{commit}:{role}").as_bytes(),
        ),
    )
}
fn merge_conflict(message: &str) -> StorageError {
    StorageError::Conflict(message.into())
}
#[allow(
    clippy::needless_pass_by_value,
    reason = "Preserve the by-value Result::map_err adapter contract for serialization errors."
)]
fn merge_encoding(error: serde_json::Error) -> StorageError {
    StorageError::SerializationError(error.to_string())
}

#[cfg(test)]
mod canonical_merge_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::{EntityRepository, RelRepository, StorageConfig, SystemEventRepository};
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::{entity::EntityType, rel::ClaimStatus, Rel, SpaceConfig};

    async fn client(embedded: bool) -> StorageClient {
        let client = if embedded {
            StorageClient::new_embedded().await
        } else {
            StorageClient::new(StorageConfig::default()).await
        }
        .unwrap();
        if !embedded {
            client
                .query_checked(concat!(
                    include_str!("../../migrations/sql/031_source_publication.surql"),
                    "\n",
                    include_str!("../../migrations/sql/046_source_support_index_fence.surql")
                ))
                .await
                .unwrap();
            crate::reference_fence::install_native_reference_guards(&client, &[])
                .await
                .unwrap();
        }
        let config: SpaceConfig = serde_json::from_value(serde_json::json!({
            "id":RecordId::new_v4(TableName::SpaceConfig), "space_id":"home",
            "created_at":Utc::now(), "updated_at":Utc::now(), "created_by":"test",
            "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
            "provenance":{},"subjects":[],"subjects_state":"complete",
            "embedding_provider":"fake","embedding_model":"test","embedding_dims":8
        }))
        .unwrap();
        crate::SpaceConfigRepository::new(client.clone())
            .create(config)
            .await
            .unwrap();
        client
    }

    fn entity(name: &str) -> Entity {
        Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "home".into(),
                "test".into(),
            ),
            EntityType::new("person"),
            name.into(),
        )
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep entities in the original fixture scope through assertions and cleanup."
    )]
    async fn fixtures(storage: &StorageClient) -> (Entity, Entity, Rel) {
        let entities = EntityRepository::new(storage.clone());
        let mut a = entity("Alice A");
        a.metadata = serde_json::json!({"angel_protectees":["a"],"nested":{"baseline":"keep"}});
        let a = entities.create(a).await.unwrap();
        let mut b = entity("Alice B");
        b.metadata = serde_json::json!({"angel_protectees":["b"]});
        b.base.security.handling.push("restricted".into());
        let b = entities.create(b).await.unwrap();
        let rel = Rel {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Rel),
                "home".into(),
                "test".into(),
            ),
            subject: Link::new(b.base.id.clone()),
            object: Link::new(b.base.id.clone()),
            rel_type: "knows".into(),
            status: ClaimStatus::Approved,
            confidence: 1.0,
            validity_window: None,
            evidence: Vec::new(),
            temporal_audit: None,
        };
        let rel = RelRepository::new(storage.clone())
            .create(rel)
            .await
            .unwrap();
        (a, b, rel)
    }

    async fn accepted_merge_input(
        storage: &StorageClient,
        text: &str,
    ) -> crate::source_intake::AcceptedSourceIntake {
        use crate::source_intake::{IntakeControl, PreparedSourceIntake};
        use elephant_types::records::{ManualDocTemplate, Source, SourceType};
        use sha2::{Digest, Sha256};
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "home".into(),
                "test".into(),
            ),
            SourceType::Manual,
            format!("{:x}", Sha256::digest(text.as_bytes())),
        );
        source
            .set_manual_doc_template(&ManualDocTemplate {
                doc_id: RecordId::new_v4(TableName::Doc).to_string(),
                title: "Independent merge evidence".into(),
                text: text.into(),
                sidecar_text: None,
                media_attachments: vec![],
                rendition_key: uuid::Uuid::new_v4().to_string(),
                metadata: serde_json::json!({}),
            })
            .unwrap();
        let prepared = Box::pin(PreparedSourceIntake::prepare(
            storage,
            source,
            IntakeControl::default(),
        ))
        .await
        .unwrap();
        Box::pin(prepared.accept(storage)).await.unwrap()
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep storage, entities, prepared, publications in the original fixture scope through assertions and cleanup."
    )]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep this ordered conformance scenario and its failure assertions in one function."
    )]
    async fn rewritten_facts_keep_physical_evidence(embedded: bool) {
        use elephant_types::records::assertion::{
            AssertionRetryState, EvidenceSpan, ResolutionStatus,
        };
        use elephant_types::records::doc_revision::RendererMetadata;
        use elephant_types::records::{Assertion, DocRevision, RevisionStatus};
        use sha2::{Digest, Sha256};
        let storage = client(embedded).await;
        let input_a = Box::pin(accepted_merge_input(&storage, "Alice lives in Paris.")).await;
        let input_b = Box::pin(accepted_merge_input(&storage, "Alice lives in Stockholm.")).await;
        let doc_b = input_b.document.as_ref().unwrap();
        assert_eq!(doc_b.source.as_ref().unwrap().id(), &input_b.source.base.id);
        let entities = EntityRepository::new(storage.clone());
        let protectee = entities.create(entity("Restricted subject")).await.unwrap();
        let mut a = entity("Alice A");
        a.base.provenance.sources = vec![input_a.source.base.id.clone()];
        a.base.security.level = 4;
        a.base.security.labels = vec!["personal".into()];
        a.base.security.handling = vec!["restricted".into()];
        a.base.subjects = vec![Link::new(protectee.base.id.clone())];
        a.base.subjects_state = SubjectsState::Partial;
        let a = entities.create(a).await.unwrap();
        let mut b = entity("Alice B");
        b.base.provenance.sources = vec![input_b.source.base.id.clone()];
        b.base.subjects_state = SubjectsState::Complete;
        let b = entities.create(b).await.unwrap();
        // A typed evidence fixture for read-only merge preparation. No render
        // completion, published head, source success, or attempt is fabricated.
        let text = doc_b.text.clone();
        let mut revision_base = doc_b.base.clone();
        revision_base.id = RecordId::new_v4(TableName::DocRevision);
        let revision = crate::DocRevisionRepository::new(storage.clone())
            .create(DocRevision {
                base: revision_base,
                doc: Link::new(doc_b.base.id.clone()),
                text_hash: format!("{:x}", Sha256::digest(text.as_bytes())),
                text: text.clone(),
                renderer: RendererMetadata {
                    name: "merge-input-fixture".into(),
                    version: "1".into(),
                    config_hash: None,
                },
                manifest: None,
                supersedes: None,
                status: RevisionStatus::Active,
                rendered_at: Utc::now(),
            })
            .await
            .unwrap();
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Assertion),
            "home".into(),
            "test".into(),
        );
        base.provenance.sources = vec![input_b.source.base.id.clone()];
        base.provenance.prompt_id = Some("original-evidence-extractor".into());
        base.subjects = vec![Link::new(b.base.id.clone())];
        base.subjects_state = SubjectsState::Complete;
        let assertion = crate::AssertionRepository::new(storage.clone())
            .create(Assertion {
                base,
                subject_text: "Alice".into(),
                predicate_raw: "lives_in".into(),
                predicate_id: None,
                object_text: None,
                object_literal: Some(serde_json::json!("Stockholm")),
                evidence: EvidenceSpan {
                    revision: Link::new(revision.base.id.clone()),
                    node: None,
                    start: 0,
                    end: u32::try_from(text.len()).unwrap(),
                    quote_hash: format!("{:x}", Sha256::digest(text.as_bytes())),
                },
                source_event_time: None,
                observed_at: Utc::now(),
                asserted_at: Utc::now(),
                assertion_key: uuid::Uuid::new_v4().to_string(),
                resolution_status: ResolutionStatus::Resolved,
                resolution_method: Some("explicit-fixture-identity".into()),
                resolution_confidence: Some(1.0),
                retry_state: AssertionRetryState::default(),
                subject_entity: Some(Link::new(b.base.id.clone())),
                object_entity: None,
                confidence: 1.0,
                subject_entity_type: Some("person".into()),
                object_entity_type: None,
                significance: None,
                lifecycle_tier: "active".into(),
            })
            .await
            .unwrap();
        let rel = RelRepository::new(storage.clone())
            .create(Rel {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::Rel),
                    "home".into(),
                    "test".into(),
                ),
                subject: Link::new(b.base.id.clone()),
                object: Link::new(protectee.base.id.clone()),
                rel_type: "knows".into(),
                status: ClaimStatus::Approved,
                confidence: 1.0,
                validity_window: None,
                evidence: vec![],
                temporal_audit: None,
            })
            .await
            .unwrap();
        let prepared = Box::pin(EntityMergeRepository::new(storage.clone()).prepare_merge(
            "home",
            &a.base.id,
            &b.base.id,
            MergeSelection::KeepA,
            "authorized-test",
        ))
        .await
        .unwrap();
        let survivor = &prepared.outcome().survivor;
        assert_eq!(
            survivor.base.provenance.sources,
            vec![
                input_a.source.base.id.clone(),
                input_b.source.base.id.clone()
            ]
        );
        let proposed = prepared
            .request
            .plan
            .effects
            .iter()
            .find_map(|effect| match &effect.after {
                Some(CanonicalRecord::Assertion(row)) if row.base.id == assertion.base.id => {
                    Some(row)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(proposed.subject_entity.as_ref().unwrap().id(), &a.base.id);
        assert_eq!(proposed.evidence, assertion.evidence);
        assert_eq!(proposed.base.provenance, assertion.base.provenance);
        assert_eq!(proposed.base.security, a.base.security);
        assert_eq!(proposed.base.subjects_state, SubjectsState::Partial);
        assert_eq!(
            proposed.base.subjects,
            vec![
                Link::new(a.base.id.clone()),
                Link::new(protectee.base.id.clone())
            ]
        );
        let moved_rel = prepared
            .request
            .plan
            .effects
            .iter()
            .find_map(|effect| match &effect.after {
                Some(CanonicalRecord::Rel(row)) if row.base.id == rel.base.id => Some(row),
                _ => None,
            })
            .unwrap();
        assert_eq!(moved_rel.subject.id(), &a.base.id);
        assert_eq!(
            moved_rel.base.provenance, rel.base.provenance,
            "manual facts do not acquire synthetic source support"
        );
        assert_eq!(moved_rel.base.security, a.base.security);
        let authorizations = prepared.authorizations().unwrap();
        let assertion_envelopes: Vec<_> = authorizations
            .iter()
            .filter(|row| row.id == assertion.base.id.to_string())
            .collect();
        assert_eq!(
            assertion_envelopes.len(),
            2,
            "old and new envelopes both require authorization"
        );
        assert!(assertion_envelopes
            .iter()
            .any(|row| row.security == proposed.base.security
                && row.subjects == proposed.base.subjects
                && row.subjects_state == proposed.base.subjects_state
                && row.read_scope == "read:assertion"
                && row.write_scope == "write:assertion"));
        assert_eq!(
            crate::AssertionRepository::new(storage.clone())
                .get("home", &assertion.base.id)
                .await
                .unwrap()
                .unwrap(),
            assertion
        );
        assert_eq!(
            crate::DocRepository::new(storage.clone())
                .get("home", &doc_b.base.id)
                .await
                .unwrap()
                .unwrap(),
            *doc_b
        );
        let publications = crate::source_publication::SourcePublicationRepository::new(storage);
        for accepted in [&input_a, &input_b] {
            assert_eq!(
                publications
                    .operation("home", &accepted.operation.operation_id)
                    .await
                    .unwrap()
                    .unwrap(),
                accepted.operation,
                "preparation must not publish either pending source"
            );
        }
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep canonical, prepared, stale in the original fixture scope through assertions and cleanup."
    )]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep this ordered conformance scenario and its failure assertions in one function."
    )]
    async fn original_plan_overlay(embedded: bool) {
        let storage = client(embedded).await;
        let (a, b, rel) = fixtures(&storage).await;
        let canonical = CanonicalRepository::new(storage.clone());
        let mut original = CanonicalPlan {
            observed_epoch: canonical.epoch("home").await.unwrap(),
            ..CanonicalPlan::default()
        };
        let mut staged_a = a.clone();
        staged_a.aliases.push("earlier hygiene alias".into());
        original
            .push(
                Some(CanonicalRecord::Entity(Box::new(a.clone()))),
                Some(CanonicalRecord::Entity(Box::new(staged_a))),
            )
            .unwrap();
        let mut new_rel = rel.clone();
        new_rel.base.id = RecordId::new_v4(TableName::Rel);
        original
            .push(None, Some(CanonicalRecord::Rel(Box::new(new_rel.clone()))))
            .unwrap();
        let prepared = EntityMergeRepository::new(storage.clone())
            .prepare_merge_group_on_plan(
                "home",
                &a.base.id,
                std::slice::from_ref(&b.base.id),
                MergeSelection::KeepA,
                "sweep",
                Some(&original),
            )
            .await
            .unwrap();
        assert!(prepared
            .outcome()
            .survivor
            .aliases
            .contains(&"earlier hygiene alias".into()));
        assert_eq!(prepared.outcome().relationships_repointed, 2);
        assert_eq!(
            EntityRepository::new(storage.clone())
                .get("home", &a.base.id)
                .await
                .unwrap()
                .unwrap(),
            a,
            "preparation must not write"
        );
        let plan = prepared.into_prepared_plan().unwrap();
        assert_eq!(plan.observed_epoch, original.observed_epoch);
        let a_effect = plan
            .effects
            .iter()
            .find(|effect| {
                effect
                    .after
                    .as_ref()
                    .is_some_and(|row| row.base().id == a.base.id)
            })
            .unwrap();
        assert!(same_merge_record(
            a_effect.before.as_ref().unwrap(),
            &CanonicalRecord::Entity(Box::new(a.clone()))
        )
        .unwrap());
        let created = plan
            .effects
            .iter()
            .find(|effect| {
                effect
                    .after
                    .as_ref()
                    .is_some_and(|row| row.base().id == new_rel.base.id)
            })
            .unwrap();
        assert!(created.before.is_none());
        assert_eq!(plan.outbox.len(), 1);
        canonical
            .commit(CanonicalCommit {
                space_id: "home".into(),
                commit_id: "whole-overlay-sweep".into(),
                input_fingerprint: "fixture-overlay-input".into(),
                replay_output: None,
                publication: None,
                finalization: None,
                maintenance: Some(CanonicalMaintenanceContext {
                    origin: CanonicalMaintenanceOrigin::Derived,
                    attempt: None,
                    sources: vec![],
                }),
                plan,
            })
            .await
            .unwrap();
        let moved = RelRepository::new(storage.clone())
            .get("home", &new_rel.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(moved.subject.id(), &a.base.id);
        assert_eq!(moved.object.id(), &a.base.id);
        assert!(EntityRepository::new(storage.clone())
            .get("home", &a.base.id)
            .await
            .unwrap()
            .unwrap()
            .aliases
            .contains(&"earlier hygiene alias".into()));
        let stale = EntityMergeRepository::new(storage)
            .prepare_merge_group_on_plan(
                "home",
                &a.base.id,
                std::slice::from_ref(&b.base.id),
                MergeSelection::KeepA,
                "sweep",
                Some(&original),
            )
            .await;
        assert!(stale.is_err(), "original epoch cannot be refreshed");
    }

    #[tokio::test]
    async fn embedded_merge_preserves_original_overlay() {
        original_plan_overlay(true).await;
    }
    #[tokio::test]
    async fn surreal_merge_preserves_original_overlay() {
        original_plan_overlay(false).await;
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep storage, prepared in the original fixture scope through assertions and cleanup."
    )]
    async fn complete_bundle(embedded: bool) {
        let storage = client(embedded).await;
        let (a, b, rel) = fixtures(&storage).await;
        let prepared = EntityMergeRepository::new(storage.clone())
            .prepare_merge(
                "home",
                &a.base.id,
                &b.base.id,
                MergeSelection::KeepA,
                "authorized-test",
            )
            .await
            .unwrap();
        let envelopes = prepared.authorizations().unwrap();
        assert!(envelopes.iter().any(|row| row.id == rel.base.id.to_string()
            && row.security.handling.contains(&"restricted".into())));
        let result = prepared.commit().await.unwrap();
        assert_eq!(result.survivor.base.id, a.base.id);
        assert_eq!(result.relationships_repointed, 1); // Self edge counted once.
        assert_eq!(
            result.survivor.metadata["angel_protectees"],
            serde_json::json!(["a", "b"])
        );
        let moved = RelRepository::new(storage.clone())
            .get("home", &rel.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(moved.subject.id(), &a.base.id);
        assert_eq!(moved.object.id(), &a.base.id);
        assert!(moved.base.security.handling.contains(&"restricted".into()));
        let redirected = EntityRepository::new(storage.clone())
            .get("home", &b.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(redirected.redirect_to.as_ref().unwrap().id(), &a.base.id);
        assert_eq!(
            EntityMergeRepository::new(storage.clone())
                .list("home", 100, None)
                .await
                .unwrap()
                .len(),
            1
        );
        let events = SystemEventRepository::new(storage.clone())
            .list("home", 100, None)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "entity.merged");
        assert!(events[0].seq > 0);
        let audits = crate::AuditLogRepository::new(storage)
            .list("home", 100, None)
            .await
            .unwrap();
        assert!(audits.iter().any(|audit| audit.record == rel.base.id));
        assert!(audits.iter().any(|audit| audit.record == b.base.id));
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Verify merge audit authority and atomic refusal against the same original plan."
    )]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep shared storage and repositories in the original fixture scope through audit and rollback assertions."
    )]
    async fn rewritten_subject_audit_authority(embedded: bool) {
        let storage = client(embedded).await;
        let entities = EntityRepository::new(storage.clone());
        let a = entities.create(entity("Survivor")).await.unwrap();
        let b = entities.create(entity("Merged")).await.unwrap();
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Doc),
            "home".into(),
            "test".into(),
        );
        base.subjects = vec![Link::new(b.base.id.clone())];
        base.subjects_state = SubjectsState::Complete;
        base.security.level = 3;
        let docs = crate::DocRepository::new(storage.clone());
        let doc = docs
            .create(elephant_types::records::Doc {
                base,
                title: "Retained document".into(),
                text: "Original identity reference".into(),
                sidecar_text: None,
                source: None,
                media_attachments: vec![],
                rendition_key: "merge-audit-subject".into(),
                metadata: serde_json::json!({}),
            })
            .await
            .unwrap();
        docs.delete("home", &doc.base.id).await.unwrap();
        let original = docs
            .get_with_deleted("home", &doc.base.id)
            .await
            .unwrap()
            .unwrap();
        let plan = EntityMergeRepository::new(storage.clone())
            .prepare_merge(
                "home",
                &a.base.id,
                &b.base.id,
                MergeSelection::KeepA,
                "test",
            )
            .await
            .unwrap()
            .into_prepared_plan()
            .unwrap();
        let audit_index = plan.effects.iter().position(|effect| matches!(
            &effect.after, Some(CanonicalRecord::AuditLog(audit)) if audit.record == doc.base.id
        )).unwrap();
        let Some(CanonicalRecord::AuditLog(audit)) = &plan.effects[audit_index].after else {
            unreachable!("selected audit");
        };
        let audit_id = audit.base.id.clone();
        assert_eq!(audit.base.subjects.len(), 2);
        assert!(audit.base.subjects.contains(&Link::new(a.base.id.clone())));
        assert!(audit.base.subjects.contains(&Link::new(b.base.id.clone())));
        assert_eq!(audit.base.security, doc.base.security);
        assert_eq!(
            audit.before.as_ref().unwrap()["subjects"],
            serde_json::json!([b.base.id])
        );
        assert_eq!(
            audit.after.as_ref().unwrap()["subjects"],
            serde_json::json!([a.base.id])
        );
        let canonical = CanonicalRepository::new(storage.clone());
        let request = CanonicalCommit {
            space_id: "home".into(),
            commit_id: "merge-audit-authority-control".into(),
            input_fingerprint: "original-subject-union".into(),
            replay_output: None,
            publication: None,
            finalization: None,
            maintenance: Some(CanonicalMaintenanceContext {
                origin: CanonicalMaintenanceOrigin::Derived,
                attempt: None,
                sources: vec![],
            }),
            plan,
        };
        for invalid_target in [false, true] {
            let mut invalid = request.clone();
            let Some(CanonicalRecord::AuditLog(audit)) =
                &mut invalid.plan.effects[audit_index].after
            else {
                unreachable!("selected audit")
            };
            if invalid_target {
                audit.record = RecordId::new_v4(TableName::Doc);
            } else {
                audit
                    .base
                    .subjects
                    .retain(|subject| subject.id() != &b.base.id);
            }
            assert!(canonical.commit(invalid).await.is_err());
            assert_eq!(
                canonical.epoch("home").await.unwrap(),
                request.plan.observed_epoch
            );
            assert_eq!(
                docs.get_with_deleted("home", &doc.base.id)
                    .await
                    .unwrap()
                    .unwrap(),
                original
            );
            assert!(crate::AuditLogRepository::new(storage.clone())
                .get("home", &audit_id)
                .await
                .unwrap()
                .is_none());
        }
        canonical.commit(request).await.unwrap();
        let restored = docs.restore("home", &doc.base.id).await.unwrap();
        assert_eq!(restored.base.subjects, vec![Link::new(a.base.id)]);
        let audit = crate::AuditLogRepository::new(storage)
            .get("home", &audit_id)
            .await
            .unwrap()
            .unwrap();
        assert!(audit.base.subjects.contains(&Link::new(b.base.id)));
    }

    #[tokio::test]
    async fn embedded_merge_audit_retains_rewritten_subject_authority() {
        rewritten_subject_audit_authority(true).await;
    }

    #[tokio::test]
    async fn surreal_merge_audit_retains_rewritten_subject_authority() {
        rewritten_subject_audit_authority(false).await;
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository, prepared in the original fixture scope through assertions and cleanup."
    )]
    async fn rollback_and_stale_read(embedded: bool) {
        let storage = client(embedded).await;
        let (a, b, rel) = fixtures(&storage).await;
        let repository = EntityMergeRepository::new(storage.clone());
        for fault in [
            crate::canonical_commit::CanonicalFault::AfterFirstEffect,
            crate::canonical_commit::CanonicalFault::BeforeOutbox,
            crate::canonical_commit::CanonicalFault::BeforeReceipt,
        ] {
            let mut prepared = repository
                .prepare_merge(
                    "home",
                    &a.base.id,
                    &b.base.id,
                    MergeSelection::KeepA,
                    "authorized-test",
                )
                .await
                .unwrap();
            prepared.fault = Some(fault);
            assert!(prepared.commit().await.is_err());
            assert_eq!(
                EntityRepository::new(storage.clone())
                    .get("home", &a.base.id)
                    .await
                    .unwrap()
                    .unwrap(),
                a
            );
            assert_eq!(
                EntityRepository::new(storage.clone())
                    .get("home", &b.base.id)
                    .await
                    .unwrap()
                    .unwrap(),
                b
            );
            assert_eq!(
                RelRepository::new(storage.clone())
                    .get("home", &rel.base.id)
                    .await
                    .unwrap()
                    .unwrap(),
                rel
            );
            assert!(SystemEventRepository::new(storage.clone())
                .list("home", 100, None)
                .await
                .unwrap()
                .is_empty());
            assert!(crate::AuditLogRepository::new(storage.clone())
                .list("home", 100, None)
                .await
                .unwrap()
                .is_empty());
        }
        let prepared = repository
            .prepare_merge(
                "home",
                &a.base.id,
                &b.base.id,
                MergeSelection::KeepA,
                "authorized-test",
            )
            .await
            .unwrap();
        let mut changed = rel.clone();
        changed.base.security.labels.push("concurrent".into());
        RelRepository::new(storage.clone())
            .update("home", &rel.base.id, changed)
            .await
            .unwrap();
        assert!(matches!(
            prepared.commit().await,
            Err(StorageError::Conflict(_))
        ));
        assert_eq!(
            EntityRepository::new(storage.clone())
                .get("home", &a.base.id)
                .await
                .unwrap()
                .unwrap(),
            a
        );
        assert!(repository.list("home", 100, None).await.unwrap().is_empty());
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep combined, retried in the original fixture scope through assertions and cleanup."
    )]
    async fn combine_and_group(embedded: bool) {
        let storage = client(embedded).await;
        let (a, b, _) = fixtures(&storage).await;
        let repository = EntityMergeRepository::new(storage.clone());
        let combined = repository
            .prepare_merge(
                "home",
                &a.base.id,
                &b.base.id,
                MergeSelection::Combine,
                "authorized-test",
            )
            .await
            .unwrap();
        let retried = repository
            .prepare_merge(
                "home",
                &a.base.id,
                &b.base.id,
                MergeSelection::Combine,
                "authorized-test",
            )
            .await
            .unwrap();
        assert_eq!(
            combined.outcome().survivor.base.id,
            retried.outcome().survivor.base.id
        );
        let result = combined.commit().await.unwrap();
        assert_ne!(result.survivor.base.id, a.base.id);
        assert_ne!(result.survivor.base.id, b.base.id);
        assert_eq!(result.merged.len(), 2);
        let c = EntityRepository::new(storage.clone())
            .create(entity("C"))
            .await
            .unwrap();
        let d = EntityRepository::new(storage.clone())
            .create(entity("D"))
            .await
            .unwrap();
        let result2 = repository
            .prepare_merge_group(
                "home",
                &result.survivor.base.id,
                &[c.base.id, d.base.id],
                MergeSelection::KeepA,
                "authorized-test",
            )
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
        assert_eq!(result2.survivor.base.id, result.survivor.base.id);
        assert_eq!(result2.merged.len(), 2);
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep storage, mutation, publication in the original fixture scope through assertions and cleanup."
    )]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep this ordered conformance scenario and its failure assertions in one function."
    )]
    async fn erase_input_preserves_independent_winner(embedded: bool) {
        use crate::source_publication::{
            AcceptSourceRevision, SourceMutation, SourcePublicationRepository,
        };
        use elephant_types::publication::SourcePrecondition;
        use elephant_types::records::{Source, SourceType};
        let storage = client(embedded).await;
        let (a, mut b, _) = fixtures(&storage).await;
        // Retained source-first state is explicitly supported by A's Legacy
        // maintenance fence; this test exercises its real erasure inverse.
        let source = crate::SourceRepository::new(storage.clone())
            .create(Source::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Source),
                    "home".into(),
                    "test".into(),
                ),
                SourceType::Manual,
                "merge-source".into(),
            ))
            .await
            .unwrap();
        b.base.provenance.sources.push(source.base.id.clone());
        b.metadata = serde_json::json!({"nested":{"source_field":"ERASE-merge-source"}});
        EntityRepository::new(storage.clone())
            .update("home", &b.base.id.clone(), b.clone())
            .await
            .unwrap();
        let merged = EntityMergeRepository::new(storage.clone())
            .prepare_merge(
                "home",
                &a.base.id,
                &b.base.id,
                MergeSelection::KeepA,
                "test",
            )
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
        let mutation = crate::public_mutation::PublicMutation::begin(storage.clone(), "home")
            .await
            .unwrap();
        let observed = EntityRepository::new(storage.clone())
            .get("home", &a.base.id)
            .await
            .unwrap()
            .unwrap();
        let mut independent = observed.clone();
        independent.metadata["nested"]["independent_after_merge"] = serde_json::json!("preserve");
        mutation
            .replace(&observed, independent, false, "independent-author")
            .await
            .unwrap();
        let publication = SourcePublicationRepository::new(storage.clone());
        let receipt = publication
            .accept(AcceptSourceRevision {
                source: source.clone(),
                revision_id: "r1".into(),
                expected: SourcePrecondition::Absent,
                idempotency_key: "accept-merge-source".into(),
                request_fingerprint: "merge-source".into(),
                reactivate: false,
                recreate_after_erasure: false,
                authorized: publication
                    .authorization_snapshot("home", &source.base.id)
                    .await
                    .unwrap(),
                authorized_document: None,
            })
            .await
            .unwrap();
        // New decision outputs have no independent baseline. The native bridge
        // must preserve their exact canonical fingerprints before any erasure.
        let decisions: Vec<crate::source_decision::SourceDecisionReceipt> = publication
            .list_controls(crate::source_decision::DECISION_TABLE, Some("home"))
            .await
            .unwrap();
        for decision in decisions {
            for effect in decision
                .effects
                .iter()
                .filter(|effect| effect.before.is_none())
            {
                let Some(after) = &effect.after else { continue };
                let current = CanonicalRecord::load(&storage, "home", &after.base().id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    current.fingerprint().unwrap(),
                    after.fingerprint().unwrap(),
                    "created decision output changed before erasure: {}\nexpected={}\nactual={}",
                    after.base().id,
                    serde_json::to_string(after).unwrap(),
                    serde_json::to_string(&current).unwrap()
                );
            }
        }
        publication
            .erase_live(SourceMutation {
                space_id: "home".into(),
                source_id: source.base.id.clone(),
                expected: SourcePrecondition::Generation(receipt.source_generation),
                idempotency_key: "erase-merge-source".into(),
                request_fingerprint: "erase-merge-source".into(),
                actor: "test".into(),
                authorized: publication
                    .authorization_snapshot("home", &source.base.id)
                    .await
                    .unwrap()
                    .unwrap(),
                authorized_document: None,
            })
            .await
            .unwrap();
        let restored = EntityRepository::new(storage.clone())
            .get("home", &a.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(restored.base.id, merged.survivor.base.id);
        assert_eq!(restored.metadata["nested"]["baseline"], "keep");
        assert_eq!(
            restored.metadata["nested"]["independent_after_merge"],
            "preserve"
        );
        assert!(restored.metadata["nested"].get("source_field").is_none());
        assert!(!restored
            .base
            .security
            .handling
            .contains(&"restricted".into()));
        let inventory = publication.erasure_inventory_rows("home").await.unwrap();
        assert!(!serde_json::to_string(&inventory)
            .unwrap()
            .contains("ERASE-merge-source"));
    }

    macro_rules! backends {
        ($embedded:ident, $surreal:ident, $body:ident) => {
            #[tokio::test]
            async fn $embedded() {
                $body(true).await;
            }
            #[tokio::test]
            async fn $surreal() {
                $body(false).await;
            }
        };
    }
    backends!(
        embedded_merge_preserves_physical_assertion_provenance,
        surreal_merge_preserves_physical_assertion_provenance,
        rewritten_facts_keep_physical_evidence
    );
    backends!(
        embedded_complete_merge_bundle,
        surreal_complete_merge_bundle,
        complete_bundle
    );
    backends!(
        embedded_merge_rollback_and_stale_read,
        surreal_merge_rollback_and_stale_read,
        rollback_and_stale_read
    );
    backends!(
        embedded_combine_and_multi_merge,
        surreal_combine_and_multi_merge,
        combine_and_group
    );
    backends!(
        embedded_merge_source_erasure_inverse,
        surreal_merge_source_erasure_inverse,
        erase_input_preserves_independent_winner
    );
}
