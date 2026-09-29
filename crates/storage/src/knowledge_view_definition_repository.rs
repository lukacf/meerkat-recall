//! Storage for governed graph-view definitions.

use chrono::{DateTime, Utc};
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::{
    Entity, KnowledgeViewDefinition, KnowledgeViewNavigation, KnowledgeViewPublicationStatus,
    KnowledgeViewSelector,
};
use elephant_types::{Link, RecordId, TableName};
use serde::Deserialize;
use surrealdb::sql::Value;

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::require::required_field;

const DEFINITION_PROJECTION: &str = r"
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
    title,
    publication_status,
    related_articles,
    navigation,
    selector
";

#[derive(Deserialize)]
struct KnowledgeViewDefinitionDb {
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
    title: Option<String>,
    publication_status: Option<KnowledgeViewPublicationStatus>,
    related_articles: Option<Vec<Link<Entity>>>,
    navigation: Option<KnowledgeViewNavigation>,
    selector: Option<KnowledgeViewSelector>,
}

impl TryFrom<KnowledgeViewDefinitionDb> for KnowledgeViewDefinition {
    type Error = StorageError;

    fn try_from(row: KnowledgeViewDefinitionDb) -> Result<Self, Self::Error> {
        Ok(Self {
            base: BaseFields {
                id: crate::surreal::record_id_from_thing(&row.id)
                    .map_err(|error| StorageError::QueryError(error.to_string()))?,
                space_id: required_field(row.space_id, "space_id")?,
                created_at: required_field(row.created_at, "created_at")?,
                updated_at: required_field(row.updated_at, "updated_at")?,
                created_by: required_field(row.created_by, "created_by")?,
                deleted_at: row.deleted_at,
                provenance: required_field(row.provenance, "provenance")?,
                security: required_field(row.security, "security")?,
                subjects: required_field(row.subjects, "subjects")?,
                subjects_state: required_field(row.subjects_state, "subjects_state")?,
            },
            title: required_field(row.title, "title")?,
            publication_status: required_field(row.publication_status, "publication_status")?,
            related_articles: row.related_articles.unwrap_or_default(),
            navigation: row.navigation,
            selector: required_field(row.selector, "selector")?,
        })
    }
}

/// Repository for typed topic, community, and saved-query definitions.
#[derive(Clone, Debug)]
pub struct KnowledgeViewDefinitionRepository {
    client: StorageClient,
}

impl KnowledgeViewDefinitionRepository {
    /// Creates a repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    fn to_value(definition: &KnowledgeViewDefinition) -> StorageResult<Value> {
        let mut object = crate::surreal::base_fields_to_object(&definition.base)?;
        object.insert("title".to_string(), definition.title.clone().into());
        object.insert(
            "publication_status".to_string(),
            match definition.publication_status {
                KnowledgeViewPublicationStatus::Draft => "draft",
                KnowledgeViewPublicationStatus::Published => "published",
                KnowledgeViewPublicationStatus::Hidden => "hidden",
            }
            .into(),
        );
        object.insert(
            "related_articles".to_string(),
            Value::Array(crate::surreal::things_from_record_ids(
                definition
                    .related_articles
                    .iter()
                    .map(|entity| entity.id().clone()),
            )),
        );
        let navigation = definition.navigation.as_ref().map_or_else(
            || Ok(Value::None),
            |navigation| {
                let navigation = serde_json::to_value(navigation)
                    .map_err(|error| StorageError::SerializationError(error.to_string()))?;
                crate::surreal::json_to_surreal(navigation)
            },
        )?;
        object.insert("navigation".to_string(), navigation);
        let selector = serde_json::to_value(&definition.selector)
            .map_err(|error| StorageError::SerializationError(error.to_string()))?;
        object.insert(
            "selector".to_string(),
            crate::surreal::json_to_surreal(selector)?,
        );
        Ok(Value::Object(object))
    }

    /// Creates or replaces a definition after validating its bounded selector.
    pub async fn upsert(
        &self,
        definition: KnowledgeViewDefinition,
    ) -> StorageResult<KnowledgeViewDefinition> {
        validate_definition(&definition)?;
        if let Some(store) = self.client.embedded_store() {
            let id = definition.base.id.clone();
            let space_id = definition.base.space_id.clone();
            if store
                .get::<KnowledgeViewDefinition>(&space_id, &id, true)
                .await?
                .is_some()
            {
                return store.update_existing(&space_id, &id, definition).await;
            }
            return store.create(definition).await;
        }
        self.client
            .query_bind_checked(
                "UPSERT $id CONTENT $definition;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&definition.base.id),
                    "definition" => Self::to_value(&definition)?,
                },
            )
            .await?;
        Ok(definition)
    }

    /// Loads one active definition in a space.
    pub async fn get(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<KnowledgeViewDefinition>> {
        if id.table() != TableName::KnowledgeViewDefinition {
            return Err(StorageError::TypeError(
                elephant_types::TypeError::InvalidRecordId(id.to_string()),
            ));
        }
        if let Some(store) = self.client.embedded_store() {
            return store
                .get::<KnowledgeViewDefinition>(space_id, id, false)
                .await;
        }
        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {DEFINITION_PROJECTION} FROM $id WHERE space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                },
            )
            .await?;
        let rows: Vec<KnowledgeViewDefinitionDb> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!(
                "Failed to parse knowledge-view definition: {error}"
            ))
        })?;
        rows.into_iter()
            .next()
            .map(KnowledgeViewDefinition::try_from)
            .transpose()
    }

    /// Lists active definitions ordered by most recent update.
    pub async fn list(
        &self,
        space_id: &str,
        limit: usize,
    ) -> StorageResult<Vec<KnowledgeViewDefinition>> {
        let limit = limit.clamp(1, 250);
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<KnowledgeViewDefinition>(space_id, limit, None, false)
                .await;
        }
        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {DEFINITION_PROJECTION} FROM knowledge_view_definition WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY updated_at DESC LIMIT {limit}"
                ),
                crate::bindings! { "space_id" => space_id },
            )
            .await?;
        let rows: Vec<KnowledgeViewDefinitionDb> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!(
                "Failed to parse knowledge-view definitions: {error}"
            ))
        })?;
        rows.into_iter()
            .map(KnowledgeViewDefinition::try_from)
            .collect()
    }

    /// Lists an ID-ordered page of active definitions for resumable bulk
    /// materialization.
    pub async fn list_page(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<KnowledgeViewDefinition>> {
        let limit = limit.clamp(1, 250);
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<KnowledgeViewDefinition>(space_id, limit, cursor, false)
                .await;
        }
        let query = if cursor.is_some() {
            format!(
                "SELECT {DEFINITION_PROJECTION} FROM knowledge_view_definition WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {DEFINITION_PROJECTION} FROM knowledge_view_definition WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
            )
        };
        let mut bindings = crate::bindings! { "space_id" => space_id };
        if let Some(cursor) = cursor {
            bindings.insert(
                "cursor".to_string(),
                crate::surreal::thing_from_record_id(cursor).into(),
            );
        }
        let mut response = self.client.query_bind(&query, bindings).await?;
        let rows: Vec<KnowledgeViewDefinitionDb> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!(
                "Failed to parse paged knowledge-view definitions: {error}"
            ))
        })?;
        rows.into_iter()
            .map(KnowledgeViewDefinition::try_from)
            .collect()
    }
}

fn validate_definition(definition: &KnowledgeViewDefinition) -> StorageResult<()> {
    if definition.base.id.table() != TableName::KnowledgeViewDefinition
        || definition.title.trim().is_empty()
        || definition.title.len() > 200
    {
        return Err(StorageError::DataIntegrityError(
            "Knowledge-view definition requires its table ID and a title of 1-200 bytes"
                .to_string(),
        ));
    }
    validate_related_articles(&definition.related_articles)?;
    validate_navigation(definition.navigation.as_ref())?;

    let (relation_types, max_hops, max_members) = match &definition.selector {
        KnowledgeViewSelector::Topic {
            topic,
            relation_types,
            max_hops,
            max_members,
        } => {
            if topic.id().table() != TableName::Entity {
                return Err(StorageError::DataIntegrityError(
                    "Topic selector root must be an entity".to_string(),
                ));
            }
            (relation_types, *max_hops, *max_members)
        }
        KnowledgeViewSelector::Community {
            seeds,
            relation_types,
            max_hops,
            max_members,
        } => {
            let unique = seeds
                .iter()
                .map(Link::id)
                .collect::<std::collections::BTreeSet<_>>();
            if seeds.is_empty()
                || seeds.len() > 32
                || unique.len() != seeds.len()
                || usize::from(*max_members) < seeds.len()
                || seeds
                    .iter()
                    .any(|seed| seed.id().table() != TableName::Entity)
            {
                return Err(StorageError::DataIntegrityError(
                    "Community selector requires 1-32 distinct entity seeds".to_string(),
                ));
            }
            (relation_types, *max_hops, *max_members)
        }
        KnowledgeViewSelector::Query {
            entity_types,
            name_contains,
            attributes,
            scan_limit,
            max_members,
        } => {
            let invalid_text = entity_types
                .iter()
                .chain(attributes.iter().map(|filter| &filter.attr_type))
                .any(|value| value.trim().is_empty() || value.len() > 100);
            if invalid_text
                || entity_types.len() > 32
                || attributes.len() > 16
                || name_contains
                    .as_ref()
                    .is_some_and(|value| value.trim().is_empty() || value.len() > 200)
                || !(1..=10_000).contains(scan_limit)
                || !(1..=250).contains(max_members)
                || max_members > scan_limit
            {
                return Err(StorageError::DataIntegrityError(
                    "Query selector exceeds its typed text, filter, scan, or member bounds"
                        .to_string(),
                ));
            }
            return Ok(());
        }
    };

    if relation_types.len() > 32
        || relation_types
            .iter()
            .any(|value| value.trim().is_empty() || value.len() > 100)
        || !(1..=4).contains(&max_hops)
        || !(1..=250).contains(&max_members)
    {
        return Err(StorageError::DataIntegrityError(
            "Graph selector exceeds its predicate, hop, or member bounds".to_string(),
        ));
    }
    Ok(())
}

fn validate_navigation(navigation: Option<&KnowledgeViewNavigation>) -> StorageResult<()> {
    let Some(navigation) = navigation else {
        return Ok(());
    };
    let fields = [
        navigation.wiki_title.as_str(),
        navigation.section_id.as_str(),
        navigation.section_title.as_str(),
    ];
    if fields
        .iter()
        .any(|value| value.trim().is_empty() || value.len() > 200)
        || navigation.section_id.contains(['\n', '\r', '`', '<', '>'])
        || navigation.wiki_title.contains(['\n', '\r', '<', '>'])
        || navigation.section_title.contains(['\n', '\r', '<', '>'])
    {
        return Err(StorageError::DataIntegrityError(
            "Knowledge-view navigation requires bounded plain-text wiki and section labels"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_related_articles(related_articles: &[Link<Entity>]) -> StorageResult<()> {
    let related = related_articles
        .iter()
        .map(Link::id)
        .collect::<std::collections::BTreeSet<_>>();
    if related_articles.len() > 24
        || related.len() != related_articles.len()
        || related_articles
            .iter()
            .any(|entity| entity.id().table() != TableName::Entity)
    {
        return Err(StorageError::DataIntegrityError(
            "Knowledge-view related articles require at most 24 distinct entity targets"
                .to_string(),
        ));
    }
    Ok(())
}
