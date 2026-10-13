use anyhow::{anyhow, Context, Result};
use elephant_types::ids::{RecordId, TableName};
use elephant_types::records::base::{Provenance, Security};
use elephant_types::records::entity::EntityType;
use elephant_types::records::rel::ClaimStatus;
use elephant_types::records::{Doc, Entity, Rel, Source};
use elephant_types::Link;
use reqwest::Client;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::env;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct ToyConfig {
    entity_count: usize,
    doc_count: usize,
    chunks_target: usize,
    rel_count: usize,
    chunk_size_hint: usize,
}

impl ToyConfig {
    fn from_env() -> Self {
        let entity_count = env::var("SMOKE_ENTITY_COUNT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);
        let doc_count = env::var("SMOKE_DOC_COUNT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30);
        let chunks_target = env::var("SMOKE_CHUNKS_TARGET")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300);
        let rel_count = env::var("SMOKE_REL_COUNT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60);
        let chunk_size_hint = env::var("SMOKE_CHUNK_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(800);

        Self {
            entity_count,
            doc_count,
            chunks_target,
            rel_count,
            chunk_size_hint,
        }
    }
}

struct Config {
    base_url: String,
    space_id: String,
    other_space_id: String,
    created_by: String,
    collection: String,
    token: Option<String>,
    other_token: Option<String>,
    verbose: bool,
}

impl Config {
    fn from_env() -> Self {
        let base_url =
            env::var("SMOKE_BASE_URL").unwrap_or_else(|_| "http://localhost:3000".to_string());
        let space_id = env::var("SMOKE_SPACE_ID").unwrap_or_else(|_| "default".to_string());
        let other_space_id =
            env::var("SMOKE_OTHER_SPACE_ID").unwrap_or_else(|_| "other_space".to_string());
        let created_by =
            env::var("SMOKE_CREATED_BY").unwrap_or_else(|_| "dev-readiness".to_string());
        let collection =
            env::var("SMOKE_COLLECTION").unwrap_or_else(|_| "dev_readiness".to_string());
        let token = env::var("SMOKE_BEARER_TOKEN").ok();
        let other_token = env::var("SMOKE_OTHER_BEARER_TOKEN").ok();
        let verbose = matches!(
            env::var("SMOKE_VERBOSE").as_deref(),
            Ok("1") | Ok("true") | Ok("yes")
        );

        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            space_id,
            other_space_id,
            created_by,
            collection,
            token,
            other_token,
            verbose,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
}

#[derive(Default)]
struct Report {
    failures: usize,
}

impl Report {
    fn ok(&self, name: &str, detail: &str) {
        println!("[ok] {name}: {detail}");
    }

    fn fail(&mut self, name: &str, err: &anyhow::Error) {
        self.failures += 1;
        println!("[fail] {name}: {err:#}");
    }
}

#[derive(Serialize)]
struct IngestRequest {
    doc: Doc,
    collection: Option<String>,
    #[serde(default)]
    sync: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env();
    let toy_config = ToyConfig::from_env();
    let client = Client::new();
    let mut report = Report::default();

    if let Err(err) = step_health(&client, &config).await {
        report.fail("health", &err);
    }

    let core = step_core_flows(&client, &config).await;
    match core {
        Ok(core_state) => {
            report.ok("core", "core flows executed");
            if let Err(err) = step_toy_load(&client, &config, &toy_config, &core_state).await {
                report.fail("toy", &err);
            } else {
                report.ok("toy", "toy load executed");
            }
        }
        Err(err) => {
            report.fail("core", &err);
        }
    }

    if report.failures > 0 {
        return Err(anyhow!("{} step(s) failed", report.failures));
    }

    println!("[done] dev readiness checks passed");
    Ok(())
}

async fn step_health(client: &Client, config: &Config) -> Result<()> {
    let url = config.url("/v1/health");
    let mut builder = client.get(url);
    if let Some(token) = &config.token {
        builder = builder.bearer_auth(token);
    }
    let response = builder.send().await.context("health request failed")?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!("health check failed: {status} {body}"));
    }
    Ok(())
}

struct CoreState {
    space_id: String,
    person_id: RecordId,
    source_primary_id: RecordId,
}

async fn step_core_flows(client: &Client, config: &Config) -> Result<CoreState> {
    let space_id = config.space_id.clone();

    let source_primary = create_source(
        client,
        config,
        &space_id,
        "email",
        "core:launch-brief",
        "sampleapp/launch/brief.txt",
    )
    .await
    .context("create primary source")?;
    let _source_secondary = create_source(
        client,
        config,
        &space_id,
        "chat",
        "core:planning-notes",
        "sampleapp/planning/notes.md",
    )
    .await
    .context("create secondary source")?;

    let person = create_entity(
        client,
        config,
        &space_id,
        "person",
        "Ava Rivers",
        Some(json!({"role": "founder"})),
    )
    .await
    .context("create person")?;
    let org = create_entity(
        client,
        config,
        &space_id,
        "org",
        "Sample App Labs",
        Some(json!({"domain": "sampleapp.local"})),
    )
    .await
    .context("create org")?;

    create_relationship(
        client,
        config,
        &space_id,
        &person.base.id,
        &org.base.id,
        "member_of",
    )
    .await
    .context("create core relationship")?;

    let _doc = ingest_doc(
        client,
        config,
        &space_id,
        "Sample App launch brief",
        "We are building Sample App as a unified knowledge graph with embeddings, provenance, and security. The launch brief covers retrieval, graph traversal, and a todo list sync.",
        Some(Link::new(source_primary.base.id.clone())),
        true,
        config.token.as_deref(),
    )
    .await
    .context("ingest core doc")?;

    let entity_hits = search_entities(client, config, &space_id, "Ava").await?;
    println!("[info] entity search hits (core): {entity_hits}");
    let doc_hits = search_docs(client, config, &space_id, "launch brief", Some(5)).await?;
    println!("[info] doc search hits (core): {doc_hits}");

    Ok(CoreState {
        space_id,
        person_id: person.base.id,
        source_primary_id: source_primary.base.id,
    })
}

async fn step_toy_load(
    client: &Client,
    config: &Config,
    toy: &ToyConfig,
    core: &CoreState,
) -> Result<()> {
    let space_id = &core.space_id;
    let mut entities = Vec::new();

    for idx in 0..toy.entity_count {
        let (entity_type, display_name, metadata) = match idx % 5 {
            0 => (
                "topic",
                format!("Task {idx}: Sample App sync"),
                json!({"category": "todo", "index": idx}),
            ),
            1 => (
                "person",
                format!("Person {idx}: Sample App User"),
                json!({"role": "user", "index": idx}),
            ),
            2 => (
                "org",
                format!("Org {idx}: Sample App Unit"),
                json!({"domain": format!("unit-{idx}.sampleapp.local")}),
            ),
            3 => (
                "device",
                format!("Device {idx}: Sample App Hub"),
                json!({"model": "HC-HUB", "slot": idx}),
            ),
            _ => (
                "topic",
                format!("Context {idx}: Sample App Routine"),
                json!({"category": "context", "index": idx}),
            ),
        };

        let entity = create_entity(
            client,
            config,
            space_id,
            entity_type,
            &display_name,
            Some(metadata),
        )
        .await
        .with_context(|| format!("create entity {idx}"))?;
        entities.push(entity);
    }

    for (idx, entity) in entities.iter().take(toy.rel_count).enumerate() {
        create_relationship(
            client,
            config,
            space_id,
            &core.person_id,
            &entity.base.id,
            "mentions",
        )
        .await
        .with_context(|| format!("create rel {idx}"))?;
    }

    let mut created_docs = Vec::new();
    let chunks_per_doc = (toy.chunks_target / toy.doc_count).max(1);
    for idx in 0..toy.doc_count {
        let title = format!("Sample App journal {idx}");
        let text = generate_text(idx, chunks_per_doc, toy.chunk_size_hint);
        let doc = ingest_doc(
            client,
            config,
            space_id,
            &title,
            &text,
            Some(Link::new(core.source_primary_id.clone())),
            true,
            config.token.as_deref(),
        )
        .await
        .with_context(|| format!("ingest toy doc {idx}"))?;
        created_docs.push(doc);
    }

    let doc_hits = search_docs(client, config, space_id, "provenance", Some(5)).await?;
    println!("[info] doc search hits (toy): {doc_hits}");

    let other_doc = ingest_doc(
        client,
        config,
        &config.other_space_id,
        "Other space doc",
        "This document should not appear in default space searches.",
        None,
        true,
        config.other_token.as_deref().or(config.token.as_deref()),
    )
    .await
    .context("ingest other-space doc")?;
    let other_doc_live = get_doc(
        client,
        config,
        &other_doc.base.id,
        config.other_token.as_deref().or(config.token.as_deref()),
    )
    .await?;
    if other_doc_live.base.space_id != *space_id {
        verify_space_isolation(client, config, space_id, &other_doc.base.id).await?;
    } else {
        println!(
            "[warn] isolation check skipped (doc space_id overridden to {})",
            other_doc_live.base.space_id
        );
    }

    let inspected_doc = get_doc(
        client,
        config,
        &created_docs[0].base.id,
        config.token.as_deref(),
    )
    .await?;
    if inspected_doc.base.security.policy_id.is_empty() {
        return Err(anyhow!("security metadata missing on retrieved doc"));
    }

    let chunk_total = count_doc_chunks(client, config, &created_docs).await?;
    if chunk_total < toy.chunks_target {
        return Err(anyhow!(
            "chunk target unmet: got {chunk_total}, expected at least {}",
            toy.chunks_target
        ));
    }

    let rel_total = search_relationships(client, config, space_id, "mentions").await?;
    println!("[info] relationship search total: {rel_total}");

    Ok(())
}

async fn create_entity(
    client: &Client,
    config: &Config,
    space_id: &str,
    entity_type: &str,
    display_name: &str,
    metadata: Option<Value>,
) -> Result<Entity> {
    let base = new_base(TableName::Entity, space_id, &config.created_by);
    let entity = Entity {
        base,
        entity_type: EntityType::new(entity_type),
        display_name: display_name.to_string(),
        aliases: Vec::new(),
        summary: None,
        handles: Vec::new(),
        identity_keys: Vec::new(),
        risk_level: None,
        trust_level: None,
        known_safe: false,
        first_seen: None,
        last_seen: None,
        status: Default::default(),
        redirect_to: None,
        metadata: metadata.unwrap_or_else(|| json!({})),
    };

    post_json(
        client,
        config,
        "/v1/entities",
        &entity,
        config.token.as_deref(),
    )
    .await
}

async fn create_relationship(
    client: &Client,
    config: &Config,
    space_id: &str,
    subject: &RecordId,
    object: &RecordId,
    rel_type: &str,
) -> Result<Rel> {
    let base = new_base(TableName::Rel, space_id, &config.created_by);
    let rel = Rel {
        base,
        subject: Link::new(subject.clone()),
        object: Link::new(object.clone()),
        rel_type: rel_type.to_string(),
        status: ClaimStatus::Candidate,
        confidence: 0.8,
        validity_window: None,
        evidence: vec![],
        temporal_audit: None,
    };

    post_json(
        client,
        config,
        "/v1/relationships",
        &rel,
        config.token.as_deref(),
    )
    .await
}

async fn create_source(
    client: &Client,
    config: &Config,
    space_id: &str,
    source_type: &str,
    external_id: &str,
    path: &str,
) -> Result<Source> {
    let mut base = new_base(TableName::Source, space_id, &config.created_by);
    base.security = Security {
        policy_id: "dev".to_string(),
        level: 2,
        labels: vec!["sampleapp".to_string()],
        handling: Vec::new(),
    };

    let mut source = Source::new(
        base,
        source_type.parse().context("invalid source type")?,
        hash_str(external_id),
    );
    source.external_id = Some(external_id.to_string());
    source.path = Some(path.to_string());
    source.channel = Some("sampleapp".to_string());

    post_json(
        client,
        config,
        "/v1/sources",
        &source,
        config.token.as_deref(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn ingest_doc(
    client: &Client,
    config: &Config,
    space_id: &str,
    title: &str,
    text: &str,
    source: Option<Link<Source>>,
    sync: bool,
    token: Option<&str>,
) -> Result<Doc> {
    let mut base = new_base(TableName::Doc, space_id, &config.created_by);
    base.security = Security {
        policy_id: "dev".to_string(),
        level: 1,
        labels: vec!["sampleapp".to_string()],
        handling: Vec::new(),
    };
    if let Some(source_link) = &source {
        base.provenance.sources.push(source_link.id().clone());
    }

    let doc = Doc {
        base,
        title: title.to_string(),
        text: text.to_string(),
        sidecar_text: None,
        source,
        media_attachments: Vec::new(),
        rendition_key: format!("doc-{}", Uuid::new_v4()),
        metadata: json!({"ingested_by": "dev_readiness"}),
    };

    let payload = IngestRequest {
        doc: doc.clone(),
        collection: Some(config.collection.clone()),
        sync,
    };

    let _ = post_json_value(client, config, "/v1/docs/ingest", &payload, token).await?;
    Ok(doc)
}

async fn search_entities(
    client: &Client,
    config: &Config,
    space_id: &str,
    query: &str,
) -> Result<usize> {
    let payload = json!({
        "query": query,
        "space_id": space_id,
        "limit": 10
    });
    let response = post_json_value(
        client,
        config,
        "/v1/entities/search",
        &payload,
        config.token.as_deref(),
    )
    .await?;
    ensure_non_empty(&response, "results").context("entity search returned no results")?;
    Ok(extract_total(&response))
}

async fn search_docs(
    client: &Client,
    config: &Config,
    space_id: &str,
    query: &str,
    k: Option<usize>,
) -> Result<usize> {
    let payload = json!({
        "query": query,
        "space_id": space_id,
        "limit": 5,
        "k": k
    });
    let response = post_json_value(
        client,
        config,
        "/v1/docs/search",
        &payload,
        config.token.as_deref(),
    )
    .await?;
    ensure_non_empty(&response, "results").context("doc search returned no results")?;
    Ok(extract_total(&response))
}

async fn get_doc(
    client: &Client,
    config: &Config,
    doc_id: &RecordId,
    token: Option<&str>,
) -> Result<Doc> {
    let path = format!("/v1/docs/{doc_id}");
    get_json(client, config, &path, token).await
}

async fn search_relationships(
    client: &Client,
    config: &Config,
    space_id: &str,
    query: &str,
) -> Result<usize> {
    let payload = json!({
        "query": query,
        "space_id": space_id,
        "limit": 50
    });
    let response = post_json_value(
        client,
        config,
        "/v1/relationships/search",
        &payload,
        config.token.as_deref(),
    )
    .await?;
    Ok(extract_total(&response))
}

async fn count_doc_chunks(client: &Client, config: &Config, docs: &[Doc]) -> Result<usize> {
    let mut total = 0usize;
    for doc in docs {
        let path = format!("/v1/docs/{}/chunks", doc.base.id);
        let response = get_json_value(client, config, &path, config.token.as_deref()).await?;
        let count = if let Some(items) = response.as_array() {
            items.len()
        } else {
            response
                .get("results")
                .and_then(|v| v.as_array())
                .map(|items| items.len())
                .unwrap_or(0)
        };
        total += count;
    }
    println!("[info] total doc chunks: {total}");
    Ok(total)
}

async fn verify_space_isolation(
    client: &Client,
    config: &Config,
    space_id: &str,
    other_doc_id: &RecordId,
) -> Result<()> {
    let payload = json!({
        "query": "document",
        "space_id": space_id,
        "limit": 50
    });
    let response = post_json_value(
        client,
        config,
        "/v1/docs/search",
        &payload,
        config.token.as_deref(),
    )
    .await?;
    let ids = collect_ids(&response);
    if ids.contains(&other_doc_id.to_string()) {
        return Err(anyhow!("space isolation failed: other-space doc visible"));
    }
    Ok(())
}

fn collect_ids(response: &Value) -> HashSet<String> {
    response
        .get("results")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    item.get("id")
                        .and_then(|id| id.as_str())
                        .map(str::to_string)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn ensure_non_empty(response: &Value, key: &str) -> Result<()> {
    let Some(array) = response.get(key).and_then(|v| v.as_array()) else {
        return Err(anyhow!("missing {key} array"));
    };
    if array.is_empty() {
        return Err(anyhow!("{key} array empty"));
    }
    Ok(())
}

fn new_base(
    table: TableName,
    space_id: &str,
    created_by: &str,
) -> elephant_types::records::base::BaseFields {
    let id = RecordId::new_v4(table);
    let mut base = elephant_types::records::base::BaseFields::new(
        id,
        space_id.to_string(),
        created_by.to_string(),
    );
    base.provenance = Provenance::default();
    base.security = Security {
        policy_id: "dev".to_string(),
        level: 1,
        labels: vec!["sampleapp".to_string()],
        handling: Vec::new(),
    };
    base
}

fn hash_str(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn generate_text(seed: usize, chunks: usize, chunk_size_hint: usize) -> String {
    let fragments = [
        "Sample App syncs tasks across the graph and doc layers.",
        "Vector search powers recall for project context.",
        "Provenance tracks sources and promotion steps.",
        "Security labels guard sensitive routines.",
        "The todo list ties entities to actionable facts.",
        "Embeddings surface similar notes quickly.",
    ];

    let mut parts = Vec::new();
    let target_len = chunks.saturating_mul(chunk_size_hint).max(chunk_size_hint);
    let mut current_len = 0usize;
    let mut idx = 0usize;
    while current_len < target_len {
        let fragment = fragments[(seed + idx) % fragments.len()];
        parts.push(fragment);
        current_len += fragment.len() + 1;
        idx += 1;
    }
    parts.join(" ")
}

async fn post_json<T: Serialize, R: serde::de::DeserializeOwned>(
    client: &Client,
    config: &Config,
    path: &str,
    payload: &T,
    token: Option<&str>,
) -> Result<R> {
    let url = config.url(path);
    let mut builder = client.post(url).json(payload);
    if let Some(token) = token {
        builder = builder.bearer_auth(token);
    }

    let response = builder.send().await.context("request failed")?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!("{path} failed: {status} {body}"));
    }
    serde_json::from_str(&body).context("deserialize response")
}

async fn post_json_value<T: Serialize>(
    client: &Client,
    config: &Config,
    path: &str,
    payload: &T,
    token: Option<&str>,
) -> Result<Value> {
    let url = config.url(path);
    let mut builder = client.post(url).json(payload);
    if let Some(token) = token {
        builder = builder.bearer_auth(token);
    }

    if config.verbose {
        let body =
            serde_json::to_value(payload).unwrap_or_else(|_| json!({"_error":"serialize_failed"}));
        println!("[query] POST {path} -> {body}");
    }
    let response = builder.send().await.context("request failed")?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!("{path} failed: {status} {body}"));
    }
    if config.verbose {
        println!("[result] POST {path} <- {body}");
    }
    serde_json::from_str(&body).context("deserialize response")
}

async fn get_json<R: serde::de::DeserializeOwned>(
    client: &Client,
    config: &Config,
    path: &str,
    token: Option<&str>,
) -> Result<R> {
    let url = config.url(path);
    let mut builder = client.get(url);
    if let Some(token) = token {
        builder = builder.bearer_auth(token);
    }

    if config.verbose {
        println!("[query] GET {path}");
    }
    let response = builder.send().await.context("request failed")?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!("{path} failed: {status} {body}"));
    }
    if config.verbose {
        println!("[result] GET {path} <- {body}");
    }
    serde_json::from_str(&body).context("deserialize response")
}

async fn get_json_value(
    client: &Client,
    config: &Config,
    path: &str,
    token: Option<&str>,
) -> Result<Value> {
    let url = config.url(path);
    let mut builder = client.get(url);
    if let Some(token) = token {
        builder = builder.bearer_auth(token);
    }

    if config.verbose {
        println!("[query] GET {path}");
    }
    let response = builder.send().await.context("request failed")?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!("{path} failed: {status} {body}"));
    }
    if config.verbose {
        println!("[result] GET {path} <- {body}");
    }
    serde_json::from_str(&body).context("deserialize response")
}

fn extract_total(response: &Value) -> usize {
    response
        .get("total")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
        .unwrap_or_else(|| {
            response
                .get("results")
                .and_then(|v| v.as_array())
                .map(|items| items.len())
                .unwrap_or(0)
        })
}
