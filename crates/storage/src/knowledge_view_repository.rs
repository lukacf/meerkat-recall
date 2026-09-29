//! Storage and atomic publication for derived knowledge views.

use std::fmt::Write as _;

use chrono::{DateTime, Utc};
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::Entity;
use elephant_types::records::{
    CompilerReceipt, KnowledgeCitation, KnowledgeDependency, KnowledgeView, KnowledgeViewBlock,
    KnowledgeViewDependencyIndex, KnowledgeViewKind, KnowledgeViewPublicationStatus,
    KnowledgeViewRevision, KnowledgeViewSurface, ProjectionFreshness, ProjectionState,
    ValidationReceipt,
};
use elephant_types::{Link, RecordId, TableName};
use serde::Deserialize;
use surrealdb::sql::Value;

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::require::required_field;

const VIEW_PROJECTION: &str = r"
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
    key,
    kind,
    surface,
    publication_status,
    root_record,
    title,
    slug,
    security_fingerprint,
    current_revision,
    revision_count,
    state,
    state_reason,
    published_at
";

const REVISION_PROJECTION: &str = r"
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
    view_id,
    revision,
    kind,
    root_record,
    blocks,
    rendered_markdown,
    citations,
    dependencies,
    freshness,
    validation,
    compiler,
    compiled_at,
    supersedes
";

const DEPENDENCY_INDEX_PROJECTION: &str = r"
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
    view,
    revision,
    dependency_record,
    dependency_type,
    role,
    observed_updated_at
";

#[derive(Deserialize)]
struct KnowledgeViewDb {
    id: surrealdb::sql::Thing,
    space_id: Option<String>,
    created_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
    created_by: Option<String>,
    deleted_at: Option<DateTime<Utc>>,
    provenance: Option<Provenance>,
    security: Option<Security>,
    subjects: Option<Vec<Link<Entity>>>,
    subjects_state: Option<SubjectsState>,
    key: Option<String>,
    kind: Option<KnowledgeViewKind>,
    surface: Option<KnowledgeViewSurface>,
    publication_status: Option<KnowledgeViewPublicationStatus>,
    root_record: Option<String>,
    title: Option<String>,
    slug: Option<String>,
    security_fingerprint: Option<String>,
    current_revision: Option<String>,
    revision_count: Option<u64>,
    state: Option<ProjectionState>,
    state_reason: Option<String>,
    published_at: Option<DateTime<Utc>>,
}

#[derive(Deserialize)]
struct KnowledgeViewRevisionDb {
    id: surrealdb::sql::Thing,
    space_id: Option<String>,
    created_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
    created_by: Option<String>,
    deleted_at: Option<DateTime<Utc>>,
    provenance: Option<Provenance>,
    security: Option<Security>,
    subjects: Option<Vec<Link<Entity>>>,
    subjects_state: Option<SubjectsState>,
    view_id: Option<String>,
    revision: Option<u64>,
    kind: Option<KnowledgeViewKind>,
    root_record: Option<String>,
    blocks: Option<Vec<KnowledgeViewBlock>>,
    rendered_markdown: Option<String>,
    citations: Option<Vec<KnowledgeCitation>>,
    dependencies: Option<Vec<KnowledgeDependency>>,
    freshness: Option<ProjectionFreshness>,
    validation: Option<ValidationReceipt>,
    compiler: Option<CompilerReceipt>,
    compiled_at: Option<DateTime<Utc>>,
    supersedes: Option<String>,
}

#[derive(Deserialize)]
struct KnowledgeViewDependencyIndexDb {
    id: surrealdb::sql::Thing,
    space_id: Option<String>,
    created_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
    created_by: Option<String>,
    deleted_at: Option<DateTime<Utc>>,
    provenance: Option<Provenance>,
    security: Option<Security>,
    subjects: Option<Vec<Link<Entity>>>,
    subjects_state: Option<SubjectsState>,
    view: Option<Link<KnowledgeView>>,
    revision: Option<Link<KnowledgeViewRevision>>,
    dependency_record: Option<String>,
    dependency_type: Option<String>,
    role: Option<String>,
    observed_updated_at: Option<DateTime<Utc>>,
}

#[derive(Deserialize)]
struct DependentViewDb {
    view: Option<Link<KnowledgeView>>,
}

#[allow(clippy::too_many_arguments)] // Mirrors explicit DB proxy fields without serde flatten.
fn decode_base(
    id: &surrealdb::sql::Thing,
    space_id: Option<String>,
    created_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
    created_by: Option<String>,
    deleted_at: Option<DateTime<Utc>>,
    provenance: Option<Provenance>,
    security: Option<Security>,
    subjects: Option<Vec<Link<Entity>>>,
    subjects_state: Option<SubjectsState>,
) -> StorageResult<BaseFields> {
    Ok(BaseFields {
        id: crate::surreal::record_id_from_thing(id)
            .map_err(|error| StorageError::QueryError(error.to_string()))?,
        space_id: required_field(space_id, "space_id")?,
        created_at: required_field(created_at, "created_at")?,
        updated_at: required_field(updated_at, "updated_at")?,
        created_by: required_field(created_by, "created_by")?,
        deleted_at,
        provenance: required_field(provenance, "provenance")?,
        security: required_field(security, "security")?,
        subjects: required_field(subjects, "subjects")?,
        subjects_state: required_field(subjects_state, "subjects_state")?,
    })
}

impl TryFrom<KnowledgeViewDb> for KnowledgeView {
    type Error = StorageError;

    fn try_from(row: KnowledgeViewDb) -> Result<Self, Self::Error> {
        Ok(Self {
            base: decode_base(
                &row.id,
                row.space_id,
                row.created_at,
                row.updated_at,
                row.created_by,
                row.deleted_at,
                row.provenance,
                row.security,
                row.subjects,
                row.subjects_state,
            )?,
            key: required_field(row.key, "key")?,
            kind: required_field(row.kind, "kind")?,
            surface: required_field(row.surface, "surface")?,
            publication_status: required_field(row.publication_status, "publication_status")?,
            root_record: required_field(row.root_record, "root_record")?,
            title: required_field(row.title, "title")?,
            slug: required_field(row.slug, "slug")?,
            security_fingerprint: required_field(row.security_fingerprint, "security_fingerprint")?,
            current_revision: row.current_revision,
            revision_count: required_field(row.revision_count, "revision_count")?,
            state: required_field(row.state, "state")?,
            state_reason: row.state_reason,
            published_at: row.published_at,
        })
    }
}

impl TryFrom<KnowledgeViewRevisionDb> for KnowledgeViewRevision {
    type Error = StorageError;

    fn try_from(row: KnowledgeViewRevisionDb) -> Result<Self, Self::Error> {
        Ok(Self {
            base: decode_base(
                &row.id,
                row.space_id,
                row.created_at,
                row.updated_at,
                row.created_by,
                row.deleted_at,
                row.provenance,
                row.security,
                row.subjects,
                row.subjects_state,
            )?,
            view_id: required_field(row.view_id, "view_id")?,
            revision: required_field(row.revision, "revision")?,
            kind: required_field(row.kind, "kind")?,
            root_record: required_field(row.root_record, "root_record")?,
            blocks: required_field(row.blocks, "blocks")?,
            rendered_markdown: required_field(row.rendered_markdown, "rendered_markdown")?,
            citations: required_field(row.citations, "citations")?,
            dependencies: required_field(row.dependencies, "dependencies")?,
            freshness: required_field(row.freshness, "freshness")?,
            validation: required_field(row.validation, "validation")?,
            compiler: required_field(row.compiler, "compiler")?,
            compiled_at: required_field(row.compiled_at, "compiled_at")?,
            supersedes: row.supersedes,
        })
    }
}

impl TryFrom<KnowledgeViewDependencyIndexDb> for KnowledgeViewDependencyIndex {
    type Error = StorageError;

    fn try_from(row: KnowledgeViewDependencyIndexDb) -> Result<Self, Self::Error> {
        Ok(Self {
            base: decode_base(
                &row.id,
                row.space_id,
                row.created_at,
                row.updated_at,
                row.created_by,
                row.deleted_at,
                row.provenance,
                row.security,
                row.subjects,
                row.subjects_state,
            )?,
            view: required_field(row.view, "view")?,
            revision: required_field(row.revision, "revision")?,
            dependency_record: required_field(row.dependency_record, "dependency_record")?,
            dependency_type: required_field(row.dependency_type, "dependency_type")?,
            role: required_field(row.role, "role")?,
            observed_updated_at: row.observed_updated_at,
        })
    }
}

/// Repository for stable views and immutable revisions.
#[derive(Clone, Debug)]
pub struct KnowledgeViewRepository {
    client: StorageClient,
}

impl KnowledgeViewRepository {
    /// Creates a repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    fn view_to_value(view: &KnowledgeView) -> StorageResult<Value> {
        let mut object = crate::surreal::base_fields_to_object(&view.base)?;
        object.insert("key".to_string(), view.key.clone().into());
        object.insert(
            "kind".to_string(),
            crate::surreal::json_to_surreal(
                serde_json::to_value(view.kind)
                    .map_err(|error| StorageError::SerializationError(error.to_string()))?,
            )?,
        );
        object.insert(
            "surface".to_string(),
            crate::surreal::json_to_surreal(
                serde_json::to_value(view.surface)
                    .map_err(|error| StorageError::SerializationError(error.to_string()))?,
            )?,
        );
        object.insert(
            "publication_status".to_string(),
            crate::surreal::json_to_surreal(
                serde_json::to_value(view.publication_status)
                    .map_err(|error| StorageError::SerializationError(error.to_string()))?,
            )?,
        );
        object.insert("root_record".to_string(), view.root_record.clone().into());
        object.insert("title".to_string(), view.title.clone().into());
        object.insert("slug".to_string(), view.slug.clone().into());
        object.insert(
            "security_fingerprint".to_string(),
            view.security_fingerprint.clone().into(),
        );
        object.insert(
            "current_revision".to_string(),
            view.current_revision
                .clone()
                .map_or(Value::None, Value::from),
        );
        let revision_count = i64::try_from(view.revision_count).map_err(|_| {
            StorageError::SerializationError("revision_count exceeds i64".to_string())
        })?;
        object.insert("revision_count".to_string(), revision_count.into());
        object.insert(
            "state".to_string(),
            crate::surreal::json_to_surreal(
                serde_json::to_value(view.state)
                    .map_err(|error| StorageError::SerializationError(error.to_string()))?,
            )?,
        );
        object.insert(
            "state_reason".to_string(),
            view.state_reason.clone().map_or(Value::None, Value::from),
        );
        object.insert(
            "published_at".to_string(),
            view.published_at
                .map(crate::surreal::datetime_from_chrono)
                .map_or(Value::None, Value::from),
        );
        Ok(Value::Object(object))
    }

    fn revision_to_value(revision: &KnowledgeViewRevision) -> StorageResult<Value> {
        let mut object = crate::surreal::base_fields_to_object(&revision.base)?;
        object.insert("view_id".to_string(), revision.view_id.clone().into());
        let revision_number = i64::try_from(revision.revision).map_err(|_| {
            StorageError::SerializationError("revision number exceeds i64".to_string())
        })?;
        object.insert("revision".to_string(), revision_number.into());
        object.insert(
            "kind".to_string(),
            crate::surreal::json_to_surreal(
                serde_json::to_value(revision.kind)
                    .map_err(|error| StorageError::SerializationError(error.to_string()))?,
            )?,
        );
        object.insert(
            "root_record".to_string(),
            revision.root_record.clone().into(),
        );
        for (name, value) in [
            ("blocks", serde_json::to_value(&revision.blocks)),
            ("citations", serde_json::to_value(&revision.citations)),
            ("dependencies", serde_json::to_value(&revision.dependencies)),
            ("freshness", serde_json::to_value(&revision.freshness)),
            ("validation", serde_json::to_value(&revision.validation)),
            ("compiler", serde_json::to_value(&revision.compiler)),
        ] {
            let json =
                value.map_err(|error| StorageError::SerializationError(error.to_string()))?;
            object.insert(name.to_string(), crate::surreal::json_to_surreal(json)?);
        }
        object.insert(
            "rendered_markdown".to_string(),
            revision.rendered_markdown.clone().into(),
        );
        object.insert(
            "compiled_at".to_string(),
            crate::surreal::datetime_from_chrono(revision.compiled_at).into(),
        );
        object.insert(
            "supersedes".to_string(),
            revision.supersedes.clone().map_or(Value::None, Value::from),
        );
        Ok(Value::Object(object))
    }

    fn dependency_index_to_value(index: &KnowledgeViewDependencyIndex) -> StorageResult<Value> {
        let mut object = crate::surreal::base_fields_to_object(&index.base)?;
        object.insert(
            "view".to_string(),
            crate::surreal::thing_from_record_id(index.view.id()).into(),
        );
        object.insert(
            "revision".to_string(),
            crate::surreal::thing_from_record_id(index.revision.id()).into(),
        );
        object.insert(
            "dependency_record".to_string(),
            index.dependency_record.clone().into(),
        );
        object.insert(
            "dependency_type".to_string(),
            index.dependency_type.clone().into(),
        );
        object.insert("role".to_string(), index.role.clone().into());
        object.insert(
            "observed_updated_at".to_string(),
            index
                .observed_updated_at
                .map(crate::surreal::datetime_from_chrono)
                .map_or(Value::None, Value::from),
        );
        Ok(Value::Object(object))
    }

    fn dependency_indexes(
        view: &KnowledgeView,
        revision: &KnowledgeViewRevision,
    ) -> Vec<KnowledgeViewDependencyIndex> {
        revision
            .dependencies
            .iter()
            .map(|dependency| {
                let mut base = view.base.clone();
                base.id = RecordId::new(
                    TableName::KnowledgeViewDependency,
                    uuid::Uuid::new_v5(
                        &uuid::Uuid::NAMESPACE_OID,
                        format!("{}|{}", view.base.id, dependency.record_id).as_bytes(),
                    ),
                );
                base.created_at = revision.compiled_at;
                base.updated_at = revision.compiled_at;
                base.created_by.clone_from(&revision.compiler.compiler);
                base.deleted_at = None;
                KnowledgeViewDependencyIndex {
                    base,
                    view: Link::new(view.base.id.clone()),
                    revision: Link::new(revision.base.id.clone()),
                    dependency_record: dependency.record_id.clone(),
                    dependency_type: dependency.record_type.clone(),
                    role: dependency.role.clone(),
                    observed_updated_at: dependency.observed_updated_at,
                }
            })
            .collect()
    }

    /// Finds a view by its deterministic key.
    pub async fn find_by_key(
        &self,
        space_id: &str,
        key: &str,
    ) -> StorageResult<Option<KnowledgeView>> {
        if let Some(store) = self.client.embedded_store() {
            let mut views = store.scan::<KnowledgeView>().await?;
            views.retain(|view| {
                view.base.space_id == space_id && view.base.deleted_at.is_none() && view.key == key
            });
            views.sort_by_key(|view| view.base.id.to_string());
            return Ok(views.into_iter().next());
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {VIEW_PROJECTION} FROM knowledge_view WHERE space_id = $space_id AND key = $key AND deleted_at IS NONE LIMIT 1"
                ),
                crate::bindings! { "space_id" => space_id, "key" => key },
            )
            .await?;
        let rows: Vec<KnowledgeViewDb> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!("Failed to parse knowledge view: {error}"))
        })?;
        rows.into_iter()
            .next()
            .map(KnowledgeView::try_from)
            .transpose()
    }

    /// Gets a stable view by ID.
    pub async fn get_view(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<KnowledgeView>> {
        if id.table() != TableName::KnowledgeView {
            return Err(StorageError::TypeError(
                elephant_types::TypeError::InvalidRecordId(id.to_string()),
            ));
        }
        if let Some(store) = self.client.embedded_store() {
            return store.get::<KnowledgeView>(space_id, id, false).await;
        }
        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {VIEW_PROJECTION} FROM knowledge_view WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                },
            )
            .await?;
        let rows: Vec<KnowledgeViewDb> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!("Failed to parse knowledge view: {error}"))
        })?;
        rows.into_iter()
            .next()
            .map(KnowledgeView::try_from)
            .transpose()
    }

    /// Gets an immutable revision by ID.
    pub async fn get_revision(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<KnowledgeViewRevision>> {
        if id.table() != TableName::KnowledgeViewRevision {
            return Err(StorageError::TypeError(
                elephant_types::TypeError::InvalidRecordId(id.to_string()),
            ));
        }
        if let Some(store) = self.client.embedded_store() {
            return store
                .get::<KnowledgeViewRevision>(space_id, id, false)
                .await;
        }
        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {REVISION_PROJECTION} FROM knowledge_view_revision WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                },
            )
            .await?;
        let rows: Vec<KnowledgeViewRevisionDb> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!("Failed to parse knowledge view revision: {error}"))
        })?;
        rows.into_iter()
            .next()
            .map(KnowledgeViewRevision::try_from)
            .transpose()
    }

    /// Gets the current immutable revision for a view.
    pub async fn get_current_revision(
        &self,
        view: &KnowledgeView,
    ) -> StorageResult<Option<KnowledgeViewRevision>> {
        let Some(id) = view.current_revision.as_deref() else {
            return Ok(None);
        };
        let id = id.parse::<RecordId>()?;
        self.get_revision(&view.base.space_id, &id).await
    }

    /// Lists stable knowledge views in a space.
    pub async fn list_views(
        &self,
        space_id: &str,
        limit: usize,
    ) -> StorageResult<Vec<KnowledgeView>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<KnowledgeView>(space_id, limit, None, false)
                .await;
        }
        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {VIEW_PROJECTION} FROM knowledge_view WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY updated_at DESC LIMIT {limit}"
                ),
                crate::bindings! { "space_id" => space_id },
            )
            .await?;
        let rows: Vec<KnowledgeViewDb> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!("Failed to parse knowledge views: {error}"))
        })?;
        rows.into_iter().map(KnowledgeView::try_from).collect()
    }

    /// Lists stable knowledge views of the requested kinds in a space.
    ///
    /// Filtering happens before the bound is applied. This prevents a large
    /// family of entity or source projections from hiding all curated views
    /// from a bounded wiki index read.
    pub async fn list_views_by_kinds(
        &self,
        space_id: &str,
        kinds: &[KnowledgeViewKind],
        limit: usize,
    ) -> StorageResult<Vec<KnowledgeView>> {
        if kinds.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(store) = self.client.embedded_store() {
            let mut views = store.scan::<KnowledgeView>().await?;
            views.retain(|view| {
                view.base.space_id == space_id
                    && view.base.deleted_at.is_none()
                    && kinds.contains(&view.kind)
            });
            views.sort_by_key(|view| std::cmp::Reverse(view.base.updated_at));
            views.truncate(limit);
            return Ok(views);
        }
        let kind_values = kinds
            .iter()
            .map(|kind| {
                match kind {
                    KnowledgeViewKind::Article => "article",
                    KnowledgeViewKind::Entity => "entity",
                    KnowledgeViewKind::Source => "source",
                    KnowledgeViewKind::Topic => "topic",
                    KnowledgeViewKind::Community => "community",
                    KnowledgeViewKind::Query => "query",
                }
                .to_string()
            })
            .collect::<Vec<_>>();
        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {VIEW_PROJECTION} FROM knowledge_view WHERE space_id = $space_id AND kind IN $kinds AND deleted_at IS NONE ORDER BY updated_at DESC LIMIT {limit}"
                ),
                crate::bindings! { "space_id" => space_id, "kinds" => kind_values },
            )
            .await?;
        let rows: Vec<KnowledgeViewDb> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!(
                "Failed to parse kind-filtered knowledge views: {error}"
            ))
        })?;
        rows.into_iter().map(KnowledgeView::try_from).collect()
    }

    /// Lists views assigned to one human surface and editorial state.
    pub async fn list_views_for_surface(
        &self,
        space_id: &str,
        surface: KnowledgeViewSurface,
        publication_status: KnowledgeViewPublicationStatus,
        limit: usize,
    ) -> StorageResult<Vec<KnowledgeView>> {
        if let Some(store) = self.client.embedded_store() {
            let mut views = store.scan::<KnowledgeView>().await?;
            views.retain(|view| {
                view.base.space_id == space_id
                    && view.base.deleted_at.is_none()
                    && view.surface == surface
                    && view.publication_status == publication_status
            });
            views.sort_by_key(|view| std::cmp::Reverse(view.base.updated_at));
            views.truncate(limit);
            return Ok(views);
        }
        let surface = crate::surreal::json_to_surreal(
            serde_json::to_value(surface)
                .map_err(|error| StorageError::SerializationError(error.to_string()))?,
        )?;
        let publication_status = crate::surreal::json_to_surreal(
            serde_json::to_value(publication_status)
                .map_err(|error| StorageError::SerializationError(error.to_string()))?,
        )?;
        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {VIEW_PROJECTION} FROM knowledge_view WHERE space_id = $space_id AND surface = $surface AND publication_status = $publication_status AND deleted_at IS NONE ORDER BY updated_at DESC LIMIT {limit}"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "surface" => surface,
                    "publication_status" => publication_status,
                },
            )
            .await?;
        let rows: Vec<KnowledgeViewDb> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!(
                "Failed to parse surface-filtered knowledge views: {error}"
            ))
        })?;
        rows.into_iter().map(KnowledgeView::try_from).collect()
    }

    /// Changes the reader publication state of every active view rooted at one
    /// managed definition. Immutable revisions are retained for audit and can
    /// be republished by a later successful reconciliation.
    pub async fn set_publication_status_for_root_record(
        &self,
        space_id: &str,
        root_record: &str,
        publication_status: KnowledgeViewPublicationStatus,
    ) -> StorageResult<usize> {
        if let Some(store) = self.client.embedded_store() {
            let target_space_id = space_id.to_string();
            let target_root_record = root_record.to_string();
            return store
                .mutate(|mutation| {
                    let views = mutation.scan::<KnowledgeView>()?;
                    let mut changed = 0_usize;
                    for mut view in views.into_iter().filter(|view| {
                        view.base.space_id == target_space_id
                            && view.base.deleted_at.is_none()
                            && view.root_record == target_root_record
                            && view.publication_status != publication_status
                    }) {
                        view.publication_status = publication_status;
                        view.base.updated_at = chrono::Utc::now();
                        if publication_status == KnowledgeViewPublicationStatus::Hidden {
                            view.published_at = None;
                        }
                        let _: KnowledgeView = mutation.replace(view)?;
                        changed = changed.saturating_add(1);
                    }
                    Ok(changed)
                })
                .await;
        }

        let status = crate::surreal::json_to_surreal(
            serde_json::to_value(publication_status)
                .map_err(|error| StorageError::SerializationError(error.to_string()))?,
        )?;
        let published_at = if publication_status == KnowledgeViewPublicationStatus::Hidden {
            surrealdb::sql::Value::None
        } else {
            surrealdb::sql::Value::from(crate::surreal::datetime_from_chrono(chrono::Utc::now()))
        };
        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE knowledge_view SET publication_status = $publication_status, published_at = $published_at, updated_at = time::now() WHERE space_id = $space_id AND root_record = $root_record AND deleted_at IS NONE RETURN {VIEW_PROJECTION}"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "root_record" => root_record,
                    "publication_status" => status,
                    "published_at" => published_at,
                },
            )
            .await?;
        let rows: Vec<KnowledgeViewDb> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!("Failed to parse publication-state update: {error}"))
        })?;
        rows.into_iter()
            .map(KnowledgeView::try_from)
            .collect::<StorageResult<Vec<_>>>()
            .map(|views| views.len())
    }

    /// Marks every current view depending on any affected record as dirty.
    ///
    /// This operation is idempotent. It records invalidation only; publication
    /// remains an explicit snapshot-consistent compiler operation.
    pub async fn mark_dirty_for_dependencies(
        &self,
        space_id: &str,
        affected_record_ids: &[String],
        reason: &str,
    ) -> StorageResult<usize> {
        if affected_record_ids.is_empty() {
            return Ok(0);
        }
        let view_ids = if let Some(store) = self.client.embedded_store() {
            let affected = affected_record_ids
                .iter()
                .collect::<std::collections::HashSet<_>>();
            store
                .scan::<KnowledgeViewDependencyIndex>()
                .await?
                .into_iter()
                .filter(|index| {
                    index.base.space_id == space_id
                        && index.base.deleted_at.is_none()
                        && affected.contains(&index.dependency_record)
                })
                .map(|index| index.view.id().clone())
                .collect::<std::collections::BTreeSet<_>>()
        } else {
            let mut response = self
                .client
                .query_bind(
                    "SELECT view FROM knowledge_view_dependency WHERE space_id = $space_id AND dependency_record IN $affected AND deleted_at IS NONE",
                    crate::bindings! {
                        "space_id" => space_id,
                        "affected" => affected_record_ids.to_vec(),
                    },
                )
                .await?;
            let rows: Vec<DependentViewDb> = response.take(0).map_err(|error| {
                StorageError::QueryError(format!(
                    "Failed to parse dependent knowledge views: {error}"
                ))
            })?;
            rows.into_iter()
                .filter_map(|row| row.view.map(|view| view.id().clone()))
                .collect::<std::collections::BTreeSet<_>>()
        };

        let mut dirty_views = Vec::new();
        for view_id in view_ids {
            if let Some(view) = self.get_view(space_id, &view_id).await? {
                if view.state != ProjectionState::Dirty {
                    dirty_views.push(view);
                }
            }
        }

        let now = Utc::now();
        for mut view in dirty_views.iter().cloned() {
            view.state = ProjectionState::Dirty;
            view.state_reason = Some(reason.to_string());
            view.base.updated_at = now;
            if let Some(store) = self.client.embedded_store() {
                let view_id = view.base.id.clone();
                let _ = store.update_existing(space_id, &view_id, view).await?;
            } else {
                self.client
                    .query_bind_checked(
                        "UPDATE $id SET state = 'dirty', state_reason = $reason, updated_at = $now WHERE space_id = $space_id AND deleted_at IS NONE;",
                        crate::bindings! {
                            "id" => crate::surreal::thing_from_record_id(&view.base.id),
                            "space_id" => space_id,
                            "reason" => reason,
                            "now" => now,
                        },
                    )
                    .await?;
            }
        }
        Ok(dirty_views.len())
    }

    /// Atomically publishes an immutable revision and advances its stable view.
    pub async fn publish(
        &self,
        view: KnowledgeView,
        revision: KnowledgeViewRevision,
    ) -> StorageResult<(KnowledgeView, KnowledgeViewRevision)> {
        let revision_id = revision.base.id.to_string();
        if view.base.space_id != revision.base.space_id
            || view.base.security != revision.base.security
            || view.security_fingerprint != revision.compiler.policy_fingerprint
            || revision.view_id != view.base.id.to_string()
            || view.current_revision.as_deref() != Some(revision_id.as_str())
            || view.revision_count != revision.revision
            || !revision
                .dependencies
                .iter()
                .any(|dependency| dependency.record_id == view.root_record)
        {
            return Err(StorageError::Conflict(
                "Knowledge-view publication receipt does not match its view".to_string(),
            ));
        }

        let indexes = Self::dependency_indexes(&view, &revision);
        if let Some(store) = self.client.embedded_store() {
            let published_view = view.clone();
            let published_revision = revision.clone();
            let published_indexes = indexes.clone();
            store
                .mutate(move |mutation| {
                    if mutation
                        .get::<KnowledgeViewRevision>(
                            &published_revision.base.space_id,
                            &published_revision.base.id,
                            true,
                        )?
                        .is_some()
                    {
                        return Err(StorageError::Conflict(format!(
                            "Knowledge view revision already exists: {}",
                            published_revision.base.id
                        )));
                    }
                    let _ = mutation.create(published_revision.clone())?;
                    if mutation
                        .get::<KnowledgeView>(
                            &published_view.base.space_id,
                            &published_view.base.id,
                            true,
                        )?
                        .is_some()
                    {
                        let _ = mutation.replace(published_view.clone())?;
                    } else {
                        let _ = mutation.create(published_view.clone())?;
                    }
                    let old_indexes = mutation.scan::<KnowledgeViewDependencyIndex>()?;
                    for index in old_indexes.into_iter().filter(|index| {
                        index.base.space_id == published_revision.base.space_id
                            && index.view.id() == &published_view.base.id
                    }) {
                        mutation.hard_delete::<KnowledgeViewDependencyIndex>(
                            &published_revision.base.space_id,
                            &index.base.id,
                        )?;
                    }
                    for index in published_indexes {
                        let _ = mutation.create(index)?;
                    }
                    Ok(())
                })
                .await?;
            return Ok((view, revision));
        }

        let mut sql = String::from(
            "BEGIN TRANSACTION; CREATE $revision_id CONTENT $revision; UPSERT $view_id CONTENT $view; DELETE knowledge_view_dependency WHERE space_id = $space_id AND view = $view_id;",
        );
        let mut bindings = crate::bindings! {
            "revision_id" => crate::surreal::thing_from_record_id(&revision.base.id),
            "revision" => Self::revision_to_value(&revision)?,
            "view_id" => crate::surreal::thing_from_record_id(&view.base.id),
            "view" => Self::view_to_value(&view)?,
            "space_id" => view.base.space_id.clone(),
        };
        for (position, index) in indexes.iter().enumerate() {
            let id_key = format!("dependency_id_{position}");
            let value_key = format!("dependency_value_{position}");
            write!(sql, " CREATE ${id_key} CONTENT ${value_key};").map_err(|error| {
                StorageError::DataIntegrityError(format!(
                    "Failed to construct dependency publication query: {error}"
                ))
            })?;
            bindings.insert(
                id_key,
                crate::surreal::thing_from_record_id(&index.base.id).into(),
            );
            bindings.insert(value_key, Self::dependency_index_to_value(index)?);
        }
        sql.push_str(" COMMIT TRANSACTION;");
        self.client.query_bind_checked(&sql, bindings).await?;
        Ok((view, revision))
    }

    /// Lists the current reverse dependency edges for a stable view.
    pub async fn list_dependency_indexes(
        &self,
        space_id: &str,
        view_id: &RecordId,
    ) -> StorageResult<Vec<KnowledgeViewDependencyIndex>> {
        if let Some(store) = self.client.embedded_store() {
            let mut indexes = store.scan::<KnowledgeViewDependencyIndex>().await?;
            indexes.retain(|index| {
                index.base.space_id == space_id
                    && index.base.deleted_at.is_none()
                    && index.view.id() == view_id
            });
            indexes.sort_by(|left, right| left.dependency_record.cmp(&right.dependency_record));
            return Ok(indexes);
        }
        let mut response = self.client.query_bind(
            &format!("SELECT {DEPENDENCY_INDEX_PROJECTION} FROM knowledge_view_dependency WHERE space_id = $space_id AND view = $view AND deleted_at IS NONE ORDER BY dependency_record"),
            crate::bindings! {
                "space_id" => space_id,
                "view" => crate::surreal::thing_from_record_id(view_id),
            },
        ).await?;
        let rows: Vec<KnowledgeViewDependencyIndexDb> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!(
                "Failed to parse knowledge-view dependency indexes: {error}"
            ))
        })?;
        rows.into_iter()
            .map(KnowledgeViewDependencyIndex::try_from)
            .collect()
    }
}
