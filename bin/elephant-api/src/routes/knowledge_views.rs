//! REST and human-browser routes for persisted Living Wiki revisions.

use std::fmt::Write as _;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::Html,
    routing::get,
    Extension, Json, Router,
};
use elephant_knowledge_views::{SourceViewCompiler, SourceViewLimits};
use elephant_storage::{KnowledgeViewDefinitionRepository, KnowledgeViewRepository, StorageClient};
use elephant_types::records::{
    KnowledgeView, KnowledgeViewBlock, KnowledgeViewDefinition, KnowledgeViewKind,
    KnowledgeViewNavigation, KnowledgeViewPublicationStatus, KnowledgeViewRevision,
    KnowledgeViewSurface, ViewBlockKind,
};
use elephant_types::{RecordId, TableName};
use serde::{Deserialize, Serialize};

use crate::auth::Principal as AuthPrincipal;
use crate::authz::AuthzState;
use crate::routes::entities::ApiError;

/// State shared by the Living Wiki REST and browser routes.
#[derive(Clone)]
pub struct KnowledgeViewsState {
    views: KnowledgeViewRepository,
    definitions: KnowledgeViewDefinitionRepository,
    source_compiler: SourceViewCompiler,
    authz: AuthzState,
}

impl KnowledgeViewsState {
    /// Creates route state.
    #[must_use]
    pub fn new(storage: StorageClient, authz: AuthzState) -> Self {
        Self {
            views: KnowledgeViewRepository::new(storage.clone()),
            definitions: KnowledgeViewDefinitionRepository::new(storage.clone()),
            source_compiler: SourceViewCompiler::new(storage),
            authz,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    section: Option<String>,
}

const fn default_limit() -> usize {
    100
}

/// One persisted view plus its current immutable revision.
#[derive(Debug, Serialize)]
pub struct KnowledgeViewDocument {
    pub view: KnowledgeView,
    pub revision: KnowledgeViewRevision,
}

#[derive(Debug, Serialize)]
struct KnowledgeViewList {
    results: Vec<KnowledgeView>,
    total: usize,
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiError>)>;
type HtmlResult = Result<(HeaderMap, Html<String>), (StatusCode, Html<String>)>;

async fn list_views(
    State(state): State<Arc<KnowledgeViewsState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Query(query): Query<ListQuery>,
) -> ApiResult<KnowledgeViewList> {
    let space_id = require_space(&principal)?;
    if !(1..=250).contains(&query.limit) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation("limit must be between 1 and 250")),
        ));
    }
    let views = state
        .views
        .list_views(&space_id, query.limit)
        .await
        .map_err(|error| internal_error(&error))?
        .into_iter()
        .filter(|view| authorize_view(&state, &principal, view).is_ok())
        .collect::<Vec<_>>();
    Ok(Json(KnowledgeViewList {
        total: views.len(),
        results: views,
    }))
}

async fn get_view(
    State(state): State<Arc<KnowledgeViewsState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<KnowledgeViewDocument> {
    let document = load_document(&state, &principal, &id).await?;
    Ok(Json(document))
}

async fn browse_index(
    State(state): State<Arc<KnowledgeViewsState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Query(query): Query<ListQuery>,
) -> HtmlResult {
    let space_id = require_space_html(&principal)?;
    let _ = query.limit;
    let mut views = state
        .views
        .list_views_for_surface(
            &space_id,
            KnowledgeViewSurface::Wiki,
            KnowledgeViewPublicationStatus::Published,
            2_000,
        )
        .await
        .map_err(|error| html_internal_error(&error))?
        .into_iter()
        .filter(is_published_wiki)
        .filter(|view| authorize_view(&state, &principal, view).is_ok())
        .collect::<Vec<_>>();
    let navigation = load_wiki_navigation(&state, &principal, &space_id)
        .await
        .map_err(|error| html_internal_error(&error))?;
    views.sort_by(|left, right| {
        let left_navigation = navigation.get(&left.root_record);
        let right_navigation = navigation.get(&right.root_record);
        left_navigation
            .map_or((u16::MAX, u16::MAX), |value| {
                (value.section_order, value.article_order)
            })
            .cmp(&right_navigation.map_or((u16::MAX, u16::MAX), |value| {
                (value.section_order, value.article_order)
            }))
            .then_with(|| left.title.to_lowercase().cmp(&right.title.to_lowercase()))
    });
    let wiki_title = views
        .iter()
        .find_map(|view| navigation.get(&view.root_record))
        .map_or("Living Wiki", |navigation| navigation.wiki_title.as_str());
    let wiki_purpose = views
        .iter()
        .find_map(|view| navigation.get(&view.root_record))
        .map(|navigation| navigation.wiki_purpose.as_str())
        .filter(|purpose| !purpose.is_empty())
        .unwrap_or("Automatically curated, interlinked articles derived from canonical knowledge.");
    let body = render_wiki_index_sections(
        &state,
        &principal,
        &views,
        &navigation,
        query.section.as_deref(),
    )
    .await
    .map_err(|error| html_internal_error(&error))?;
    Ok(html_response(page_shell(
        wiki_title,
        &format!(
            "<header><p class=\"eyebrow\">Elephant Living Wiki</p><h1>{}</h1><p class=\"lede\">{}</p>{}</header><main>{body}</main>",
            escape_html(wiki_title),
            escape_html(wiki_purpose),
            query.section.as_ref().map_or_else(String::new, |_| "<p><a href=\"/wiki\">Back to the wiki overview</a></p>".to_string()),
        ),
    )))
}

async fn render_wiki_index_sections(
    state: &KnowledgeViewsState,
    principal: &AuthPrincipal,
    views: &[KnowledgeView],
    navigation: &std::collections::BTreeMap<String, KnowledgeViewNavigation>,
    selected_section: Option<&str>,
) -> Result<String, elephant_storage::StorageError> {
    let mut body = String::new();
    let mut current_section = None::<(u16, String)>;
    let section_counts = views
        .iter()
        .filter_map(|view| navigation.get(&view.root_record))
        .fold(
            std::collections::BTreeMap::<String, usize>::new(),
            |mut counts, item| {
                *counts.entry(item.section_id.clone()).or_default() += 1;
                counts
            },
        );
    let compact_index = selected_section.is_none();
    let mut shown_in_section = 0_usize;
    for view in views {
        let view_navigation = navigation.get(&view.root_record);
        if selected_section
            .is_some_and(|selected| view_navigation.is_none_or(|item| item.section_id != selected))
        {
            continue;
        }
        let section_order = view_navigation.map_or(u16::MAX, |value| value.section_order);
        let section_title =
            view_navigation.map_or("Articles", |value| value.section_title.as_str());
        let section_key = (section_order, section_title.to_string());
        if current_section.as_ref() != Some(&section_key) {
            if current_section.is_some() {
                body.push_str("</div></section>");
            }
            shown_in_section = 0;
            let description = view_navigation
                .map(|value| value.section_description.as_str())
                .unwrap_or_default();
            let section_id = view_navigation
                .map(|value| value.section_id.as_str())
                .unwrap_or_default();
            let article_count = section_counts.get(section_id).copied().unwrap_or_default();
            let _ = write!(
                body,
                "<section class=\"wiki-section\" id=\"section-{}\"><h2>{}</h2><p>{}</p><p><a href=\"/wiki?section={}\">Browse all {} articles</a></p><div class=\"wiki-grid\">",
                section_order,
                escape_html(section_title),
                escape_html(description),
                escape_html(section_id),
                article_count,
            );
            current_section = Some(section_key);
        }
        if compact_index
            && (shown_in_section >= 12
                || view_navigation.is_none_or(|navigation| !navigation.featured))
        {
            continue;
        }
        let lead = state
            .views
            .get_current_revision(view)
            .await?
            .filter(|revision| {
                state
                    .authz
                    .authorize_record_read(principal, revision, read_scope(view.kind))
                    .is_ok()
                    && elephant_knowledge_views::has_complete_subject_contract(&revision.base)
            })
            .and_then(|revision| article_lead(&revision));
        let _ = write!(
            body,
            "<article class=\"card\"><h2><a href=\"/wiki/{}\">{}</a></h2>{}</article>",
            escape_html(&view.base.id.to_string()),
            escape_html(&view.title),
            lead.map_or_else(String::new, |lead| format!("<p>{}</p>", escape_html(&lead))),
        );
        shown_in_section = shown_in_section.saturating_add(1);
    }
    if current_section.is_some() {
        body.push_str("</div></section>");
    }
    if views.is_empty() {
        body.push_str("<section class=\"empty\"><h2>No curated articles yet</h2><p>Elephant has evidence and graph records, but no editorially defined wiki articles are currently published. Raw entities and sources are deliberately excluded from this surface.</p></section>");
    }
    Ok(body)
}

async fn browse_view(
    State(state): State<Arc<KnowledgeViewsState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> HtmlResult {
    let document = load_document(&state, &principal, &id)
        .await
        .map_err(api_error_to_html)?;
    if !is_published_wiki(&document.view) {
        return Err(not_found_html(
            "This record is evidence or an internal projection, not a published wiki article.",
        ));
    }
    let view = &document.view;
    let revision = &document.revision;
    let warning = semantic_state_warning(view, revision);
    let article = render_semantic_blocks(&view.base.id, &revision.blocks);
    let related = load_planned_related_articles(&state, &principal, view)
        .await
        .map_err(|error| html_internal_error(&error))?;
    let related = render_related_articles(&related);
    let navigation = load_article_navigation(&state, &principal, view)
        .await
        .map_err(|error| html_internal_error(&error))?;
    let back_link = navigation.as_ref().map_or_else(
        || "<nav><a href=\"/wiki\">Back to the wiki</a></nav>".to_string(),
        |navigation| {
            format!(
                "<nav><a href=\"/wiki#section-{}\">Back to {}</a></nav>",
                navigation.section_order,
                escape_html(&navigation.section_title),
            )
        },
    );
    let eyebrow = navigation.as_ref().map_or_else(
        || article_kind_name(view.kind).to_string(),
        |value| value.section_title.clone(),
    );
    let content = format!(
        "{}<header><p class=\"eyebrow\">{}</p><h1>{}</h1>{}</header><main class=\"article\">{}{}<footer><a href=\"/inspect/knowledge-views/{}\">Evidence and technical details</a></footer></main>",
        back_link,
        escape_html(&eyebrow),
        escape_html(&view.title),
        warning,
        article,
        related,
        escape_html(&view.base.id.to_string()),
    );
    Ok(html_response(page_shell(&view.title, &content)))
}

async fn browse_evidence_index(
    State(state): State<Arc<KnowledgeViewsState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Query(query): Query<ListQuery>,
) -> HtmlResult {
    let space_id = require_space_html(&principal)?;
    let views = state
        .views
        .list_views_for_surface(
            &space_id,
            KnowledgeViewSurface::Evidence,
            KnowledgeViewPublicationStatus::Published,
            query.limit.clamp(1, 250),
        )
        .await
        .map_err(|error| html_internal_error(&error))?
        .into_iter()
        .filter(|view| view.kind == KnowledgeViewKind::Source)
        .filter(|view| authorize_view(&state, &principal, view).is_ok())
        .collect::<Vec<_>>();
    let mut body = String::new();
    for view in &views {
        let _ = write!(
            body,
            "<article class=\"card\"><p class=\"eyebrow\">Source evidence</p><h2><a href=\"/evidence/{}\">{}</a></h2></article>",
            escape_html(&view.base.id.to_string()),
            escape_html(&view.title),
        );
    }
    if views.is_empty() {
        body.push_str("<section class=\"empty\"><h2>No source evidence views yet</h2></section>");
    }
    Ok(html_response(page_shell(
        "Elephant Evidence",
        &format!(
            "<header><p class=\"eyebrow\">Elephant</p><h1>Evidence</h1><p class=\"lede\">Source material supporting the knowledge graph. Sources are not wiki articles.</p></header><main>{body}</main>"
        ),
    )))
}

async fn browse_evidence_view(
    State(state): State<Arc<KnowledgeViewsState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> HtmlResult {
    let mut document = load_document(&state, &principal, &id)
        .await
        .map_err(api_error_to_html)?;
    if document.view.surface != KnowledgeViewSurface::Evidence
        || document.view.publication_status != KnowledgeViewPublicationStatus::Published
        || document.view.kind != KnowledgeViewKind::Source
    {
        return Err(not_found_html("This record is not source evidence."));
    }
    let source_id = document
        .view
        .root_record
        .parse::<RecordId>()
        .map_err(|_| not_found_html("The source evidence root is invalid."))?;
    let compiled = state
        .source_compiler
        .compile(
            &document.view.base.space_id,
            &source_id,
            SourceViewLimits::default(),
            false,
        )
        .await
        .map_err(|error| html_internal_error(&error))?;
    authorize_view(&state, &principal, &compiled.view).map_err(api_error_to_html)?;
    state
        .authz
        .authorize_record_read(
            &principal,
            &compiled.revision,
            read_scope(compiled.view.kind),
        )
        .map_err(authz_error)
        .map_err(api_error_to_html)?;
    document = KnowledgeViewDocument {
        view: compiled.view,
        revision: compiled.revision,
    };
    let view = &document.view;
    let evidence_blocks = document
        .revision
        .blocks
        .iter()
        .filter(|block| block.id == "source-summary" || block.id.contains("excerpt"))
        .cloned()
        .collect::<Vec<_>>();
    let content = format!(
        "<nav><a href=\"/evidence\">Back to evidence</a></nav><header><p class=\"eyebrow\">Source evidence</p><h1>{}</h1><p class=\"lede\">This source supports semantic knowledge in Elephant. It is not itself a wiki article.</p></header><main class=\"article\">{}<footer><a href=\"/inspect/knowledge-views/{}\">Technical details</a></footer></main>",
        escape_html(&view.title),
        render_evidence_blocks(&view.base.id, &evidence_blocks),
        escape_html(&view.base.id.to_string()),
    );
    Ok(html_response(page_shell(&view.title, &content)))
}

async fn browse_inspector_index(
    State(state): State<Arc<KnowledgeViewsState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Query(query): Query<ListQuery>,
) -> HtmlResult {
    let space_id = require_space_html(&principal)?;
    let views = state
        .views
        .list_views(&space_id, query.limit.clamp(1, 250))
        .await
        .map_err(|error| html_internal_error(&error))?
        .into_iter()
        .filter(|view| authorize_view(&state, &principal, view).is_ok())
        .collect::<Vec<_>>();
    let mut body = String::new();
    for view in &views {
        let _ = write!(
            body,
            "<article class=\"card\"><div class=\"meta\"><span>{}</span><span class=\"state {}\">{}</span></div><h2><a href=\"/inspect/knowledge-views/{}\">{}</a></h2><p>{}</p><small>Revision {} - published {}</small></article>",
            kind_name(view.kind),
            state_name(view.state),
            state_name(view.state),
            escape_html(&view.base.id.to_string()),
            escape_html(&view.title),
            escape_html(view.state_reason.as_deref().unwrap_or("Current compiled projection")),
            view.revision_count,
            view.published_at.map_or_else(|| "not yet".to_string(), |time| time.to_rfc3339()),
        );
    }
    Ok(html_response(page_shell(
        "Knowledge view inspector",
        &format!(
            "<header><p class=\"eyebrow\">Developer surface</p><h1>Knowledge view inspector</h1><p class=\"lede\">Projection records, compilation state, and receipts.</p></header><main>{body}</main>"
        ),
    )))
}

async fn browse_inspector_view(
    State(state): State<Arc<KnowledgeViewsState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> HtmlResult {
    let document = load_document(&state, &principal, &id)
        .await
        .map_err(api_error_to_html)?;
    let view = &document.view;
    let revision = &document.revision;
    let content = format!(
        "<nav><a href=\"/inspect/knowledge-views\">Back to inspector</a></nav><header><div class=\"meta\"><span>{}</span><span class=\"state {}\">{}</span></div><h1>{}</h1><p class=\"lede\">Revision {} - {} dependencies - {} citations</p></header><main><pre class=\"wiki\">{}</pre><section class=\"receipt\"><h2>Compilation receipt</h2><dl><dt>Compiler</dt><dd>{} v{}</dd><dt>Dependency hash</dt><dd>{}</dd><dt>Compiled</dt><dd>{}</dd><dt>Selection truncated</dt><dd>{}</dd><dt>Unsupported claims</dt><dd>{}</dd></dl></section></main>",
        kind_name(view.kind),
        state_name(view.state),
        state_name(view.state),
        escape_html(&view.title),
        revision.revision,
        revision.dependencies.len(),
        revision.citations.len(),
        escape_html(&revision.rendered_markdown),
        escape_html(&revision.compiler.compiler),
        revision.compiler.version,
        escape_html(&revision.compiler.dependency_hash),
        revision.compiled_at.to_rfc3339(),
        revision.freshness.selection_truncated,
        revision.freshness.unsupported_claims,
    );
    Ok(html_response(page_shell(&view.title, &content)))
}

async fn load_document(
    state: &KnowledgeViewsState,
    principal: &AuthPrincipal,
    id: &str,
) -> Result<KnowledgeViewDocument, (StatusCode, Json<ApiError>)> {
    let space_id = require_space(principal)?;
    let id = id.parse::<RecordId>().map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(error.to_string())),
        )
    })?;
    if id.table() != TableName::KnowledgeView {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation("Expected a knowledge_view ID")),
        ));
    }
    let view = state
        .views
        .get_view(&space_id, &id)
        .await
        .map_err(|error| internal_error(&error))?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("KnowledgeView", &id.to_string())),
            )
        })?;
    authorize_view(state, principal, &view)?;
    let revision = state
        .views
        .get_current_revision(&view)
        .await
        .map_err(|error| internal_error(&error))?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("KnowledgeViewRevision", "current")),
            )
        })?;
    if !elephant_knowledge_views::has_complete_subject_contract(&revision.base) {
        return Err((StatusCode::FORBIDDEN, Json(ApiError::validation(
            "Access denied: projection revision requires recompilation to establish its subject scope",
        ))));
    }
    state
        .authz
        .authorize_record_read(principal, &revision, read_scope(view.kind))
        .map_err(authz_error)?;
    Ok(KnowledgeViewDocument { view, revision })
}

#[allow(clippy::result_large_err)]
fn authorize_view(
    state: &KnowledgeViewsState,
    principal: &AuthPrincipal,
    view: &KnowledgeView,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    if !elephant_knowledge_views::has_complete_subject_contract(&view.base) {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ApiError::validation(
                "Access denied: projection requires recompilation to establish its subject scope",
            )),
        ));
    }
    if view.kind == KnowledgeViewKind::Source {
        state
            .authz
            .authorize_record_read(principal, view, "read:doc:raw")
            .map_err(authz_error)?;
    }
    state
        .authz
        .authorize_record_read(principal, view, read_scope(view.kind))
        .map_err(authz_error)
}

const fn read_scope(kind: KnowledgeViewKind) -> &'static str {
    if matches!(kind, KnowledgeViewKind::Source) {
        "read:source"
    } else {
        "read:entity"
    }
}

#[allow(clippy::result_large_err)]
fn require_space(principal: &AuthPrincipal) -> Result<String, (StatusCode, Json<ApiError>)> {
    principal
        .space_id
        .clone()
        .filter(|space| !space.trim().is_empty())
        .ok_or_else(|| {
            (
                StatusCode::FORBIDDEN,
                Json(ApiError::validation("Access denied: missing space_id")),
            )
        })
}

fn require_space_html(principal: &AuthPrincipal) -> Result<String, (StatusCode, Html<String>)> {
    principal
        .space_id
        .clone()
        .filter(|space| !space.trim().is_empty())
        .ok_or_else(|| {
            (
                StatusCode::FORBIDDEN,
                Html(page_shell(
                    "Access denied",
                    "<h1>Access denied</h1><p>A space-scoped principal is required.</p>",
                )),
            )
        })
}

fn internal_error(error: &elephant_storage::StorageError) -> (StatusCode, Json<ApiError>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ApiError::internal(error.to_string())),
    )
}

fn authz_error(error: crate::authz::AuthzError) -> (StatusCode, Json<ApiError>) {
    (
        StatusCode::FORBIDDEN,
        Json(ApiError {
            code: error.code,
            message: error.message,
            details: None,
        }),
    )
}

fn html_internal_error(error: &elephant_storage::StorageError) -> (StatusCode, Html<String>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Html(page_shell(
            "Living Wiki error",
            &format!(
                "<h1>Could not load the wiki</h1><p>{}</p>",
                escape_html(&error.to_string())
            ),
        )),
    )
}

fn api_error_to_html(
    (status, Json(error)): (StatusCode, Json<ApiError>),
) -> (StatusCode, Html<String>) {
    (
        status,
        Html(page_shell(
            "Living Wiki error",
            &format!(
                "<h1>Could not load this view</h1><p>{}</p>",
                escape_html(&error.message)
            ),
        )),
    )
}

fn html_response(body: String) -> (HeaderMap, Html<String>) {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'"),
    );
    (headers, Html(body))
}

fn page_shell(title: &str, content: &str) -> String {
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>{}</title><style>{}</style></head><body>{}</body></html>",
        escape_html(title),
        STYLE,
        content
    )
}

const STYLE: &str = r"
:root{color-scheme:light;background:#f4f1e9;color:#162018;font:16px/1.55 ui-sans-serif,system-ui,sans-serif}body{max-width:1100px;margin:0 auto;padding:48px 24px 80px}a{color:#1c5d42;text-decoration-thickness:1px;text-underline-offset:3px}header{margin:44px 0 32px}.eyebrow,.meta{color:#667067;font-size:.78rem;font-weight:700;letter-spacing:.1em;text-transform:uppercase}.meta{display:flex;justify-content:space-between;gap:16px}h1{font-family:ui-serif,Georgia,serif;font-size:clamp(2.3rem,7vw,5.5rem);letter-spacing:-.045em;line-height:.95;margin:.2em 0}.lede{color:#596159;font-size:1.14rem;max-width:650px}.wiki-section{margin:3.5rem 0}.wiki-section>h2{font-family:ui-serif,Georgia,serif;font-size:2rem;margin:0 0 1rem}.wiki-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px}.card,.receipt,.empty{background:#fff;border:1px solid #d8d4ca;border-radius:16px;margin:0;padding:24px;box-shadow:0 10px 35px #2135260b}.card h2,.article h2{font-family:ui-serif,Georgia,serif;font-size:1.65rem;margin:.35rem 0}.card p{color:#596159}.state{border-radius:999px;padding:2px 9px;background:#e6ece6}.state.degraded,.state.dirty{background:#f6e7c9;color:#714916}.state.failed{background:#f2d5d1;color:#7d2721}.quality-warning{background:#fff4d9;border-left:4px solid #bd7b16;border-radius:4px;padding:14px 18px;max-width:700px}.article{background:#fff;border:1px solid #d8d4ca;border-radius:16px;padding:clamp(24px,5vw,52px);box-shadow:0 10px 35px #2135260b}.article section{max-width:720px;margin:0 0 2.5rem}.article section:first-child p:first-of-type{font-size:1.18rem;line-height:1.65}.article ul{padding-left:1.25rem}.article li{margin:.55rem 0}.article blockquote{background:#f4f1e9;border-left:4px solid #8ea08f;margin:1rem 0;padding:18px 22px;white-space:pre-wrap}.article footer{border-top:1px solid #e1ded6;margin-top:3rem;padding-top:1.25rem;color:#667067}.wiki{background:#172019;color:#ecf1e8;border-radius:16px;padding:28px;white-space:pre-wrap;overflow-wrap:anywhere;font:15px/1.65 ui-monospace,SFMono-Regular,Menlo,monospace}.receipt{margin-top:28px}.receipt dl{display:grid;grid-template-columns:minmax(130px,200px) 1fr;gap:8px 20px}.receipt dt{font-weight:700}.receipt dd{margin:0;overflow-wrap:anywhere}nav{margin-top:8px}@media(max-width:600px){body{padding:28px 16px}.receipt dl{grid-template-columns:1fr}.wiki{padding:18px}.article{padding:22px}}
";

const fn is_published_wiki(view: &KnowledgeView) -> bool {
    matches!(view.surface, KnowledgeViewSurface::Wiki)
        && matches!(view.kind, KnowledgeViewKind::Article)
        && matches!(
            view.publication_status,
            KnowledgeViewPublicationStatus::Published
        )
}

const fn article_kind_name(kind: KnowledgeViewKind) -> &'static str {
    match kind {
        KnowledgeViewKind::Article => "Article",
        KnowledgeViewKind::Entity
        | KnowledgeViewKind::Source
        | KnowledgeViewKind::Topic
        | KnowledgeViewKind::Community
        | KnowledgeViewKind::Query => "Internal projection",
    }
}

fn semantic_state_warning(view: &KnowledgeView, revision: &KnowledgeViewRevision) -> String {
    let freshness = &revision.freshness;
    if view.state == elephant_types::records::ProjectionState::Current
        && freshness.unsupported_claims == 0
        && freshness.unresolved_conflicts == 0
        && freshness.stale_truth_slots == 0
        && freshness.expired_claims == 0
        && !freshness.selection_truncated
    {
        return String::new();
    }

    let mut reasons = Vec::new();
    if freshness.unresolved_conflicts > 0 {
        reasons.push(format!(
            "{} unresolved conflict(s)",
            freshness.unresolved_conflicts
        ));
    }
    if freshness.unsupported_claims > 0 {
        reasons.push(format!(
            "{} claim(s) without source evidence",
            freshness.unsupported_claims
        ));
    }
    let stale = freshness
        .stale_truth_slots
        .saturating_add(freshness.expired_claims);
    if stale > 0 {
        reasons.push(format!("{stale} stale or expired claim(s)"));
    }
    if freshness.selection_truncated {
        reasons.push("the bounded selection was incomplete".to_string());
    }
    if reasons.is_empty() {
        reasons.push(
            view.state_reason
                .clone()
                .unwrap_or_else(|| "the article is being refreshed".to_string()),
        );
    }
    format!(
        "<p class=\"quality-warning\"><strong>Knowledge notice:</strong> {}. Treat affected statements as provisional.</p>",
        escape_html(&reasons.join(", "))
    )
}

fn render_semantic_blocks(view_id: &RecordId, blocks: &[KnowledgeViewBlock]) -> String {
    let mut html = String::new();
    for block in blocks.iter().filter(|block| is_semantic_block(block.kind)) {
        let statements = block
            .statements
            .iter()
            .filter_map(|statement| {
                let text = plain_statement_text(&statement.text);
                (!text.is_empty()).then_some(text)
            })
            .collect::<Vec<_>>();
        if statements.is_empty() {
            continue;
        }
        let _ = write!(
            html,
            "<section id=\"{}\"><h2>{}</h2>",
            escape_html(&block.id),
            escape_html(&block.heading)
        );
        if statements.len() == 1 && !statement_is_list_item(&block.statements[0].text) {
            let _ = write!(html, "<p>{}</p>", escape_html(&statements[0]));
        } else {
            html.push_str("<ul>");
            for statement in statements {
                let _ = write!(html, "<li>{}</li>", escape_html(&statement));
            }
            html.push_str("</ul>");
        }
        let _ = write!(
            html,
            "<p><small><a href=\"/inspect/knowledge-views/{}#{}\">Review evidence</a></small></p></section>",
            escape_html(&view_id.to_string()),
            escape_html(&block.id),
        );
    }
    if html.is_empty() {
        html.push_str("<section class=\"empty\"><h2>Article not publishable</h2><p>This projection does not yet contain semantic, statement-level content suitable for the wiki.</p></section>");
    }
    html
}

fn render_evidence_blocks(view_id: &RecordId, blocks: &[KnowledgeViewBlock]) -> String {
    let mut html = String::new();
    for block in blocks {
        if block.statements.is_empty() {
            continue;
        }
        let _ = write!(
            html,
            "<section id=\"{}\"><h2>{}</h2>",
            escape_html(&block.id),
            escape_html(&block.heading)
        );
        for statement in &block.statements {
            if let Some((label, quote)) = statement.text.split_once("\n\n> ") {
                let label = plain_statement_text(label)
                    .trim_end_matches(':')
                    .to_string();
                let quote = plain_statement_text(&quote.replace("\n> ", "\n"));
                let _ = write!(
                    html,
                    "<p><strong>{}</strong></p><blockquote>{}</blockquote>",
                    escape_html(&label),
                    escape_html(&quote),
                );
            } else {
                let statement = plain_statement_text(&statement.text);
                let _ = write!(html, "<p>{}</p>", escape_html(&statement));
            }
        }
        let _ = write!(
            html,
            "<p><small><a href=\"/inspect/knowledge-views/{}#{}\">Review provenance</a></small></p></section>",
            escape_html(&view_id.to_string()),
            escape_html(&block.id),
        );
    }
    if html.is_empty() {
        html.push_str("<section class=\"empty\"><h2>No readable source content</h2><p>Elephant has no bounded source excerpt for this record. Technical lineage remains available in the inspector.</p></section>");
    }
    html
}

async fn load_planned_related_articles(
    state: &KnowledgeViewsState,
    principal: &AuthPrincipal,
    view: &KnowledgeView,
) -> Result<Vec<KnowledgeView>, elephant_storage::StorageError> {
    let Ok(definition_id) = view.root_record.parse::<RecordId>() else {
        return Ok(Vec::new());
    };
    let Some(definition) = state
        .definitions
        .get(&view.base.space_id, &definition_id)
        .await?
    else {
        return Ok(Vec::new());
    };
    if !can_read_definition(state, principal, &definition) {
        return Ok(Vec::new());
    }
    if definition.related_articles.is_empty() {
        // Continue: another planned article may explicitly link here, which
        // becomes a graph-native backlink on this page.
    }
    let mut definitions = Vec::new();
    let mut cursor = None;
    loop {
        let page = state
            .definitions
            .list_page(&view.base.space_id, 250, cursor.as_ref())
            .await?;
        if page.is_empty() {
            break;
        }
        let last_page = page.len() < 250;
        cursor = page.last().map(|definition| definition.base.id.clone());
        definitions.extend(
            page.into_iter()
                .filter(|definition| can_read_definition(state, principal, definition)),
        );
        if last_page {
            break;
        }
    }
    let published = state
        .views
        .list_views_for_surface(
            &view.base.space_id,
            KnowledgeViewSurface::Wiki,
            KnowledgeViewPublicationStatus::Published,
            2_000,
        )
        .await?;
    let current_topic = match &definition.selector {
        elephant_types::records::KnowledgeViewSelector::Topic { topic, .. } => Some(topic.id()),
        elephant_types::records::KnowledgeViewSelector::Community { seeds, .. } => {
            seeds.first().map(elephant_types::Link::id)
        }
        elephant_types::records::KnowledgeViewSelector::Query { .. } => None,
    };
    let mut related_definitions = Vec::new();
    for target in &definition.related_articles {
        if let Some(target_definition) = definitions
            .iter()
            .find(|candidate| selector_contains_entity(&candidate.selector, target.id()))
        {
            related_definitions.push(target_definition);
        }
    }
    if let Some(current_topic) = current_topic {
        for candidate in &definitions {
            if candidate.base.id != definition.base.id
                && candidate
                    .related_articles
                    .iter()
                    .any(|target| target.id() == current_topic)
                && !related_definitions
                    .iter()
                    .any(|related| related.base.id == candidate.base.id)
            {
                related_definitions.push(candidate);
            }
        }
    }
    let mut related = Vec::new();
    for target_definition in related_definitions {
        if let Some(target_view) = published.iter().find(|candidate| {
            candidate.root_record == target_definition.base.id.to_string()
                && candidate.base.id != view.base.id
                && is_published_wiki(candidate)
                && authorize_view(state, principal, candidate).is_ok()
        }) {
            related.push(target_view.clone());
        }
    }
    related.truncate(24);
    Ok(related)
}

fn selector_contains_entity(
    selector: &elephant_types::records::KnowledgeViewSelector,
    entity_id: &RecordId,
) -> bool {
    match selector {
        elephant_types::records::KnowledgeViewSelector::Topic { topic, .. } => {
            topic.id() == entity_id
        }
        elephant_types::records::KnowledgeViewSelector::Community { seeds, .. } => {
            seeds.iter().any(|seed| seed.id() == entity_id)
        }
        elephant_types::records::KnowledgeViewSelector::Query { .. } => false,
    }
}

fn can_read_definition(
    state: &KnowledgeViewsState,
    principal: &AuthPrincipal,
    definition: &KnowledgeViewDefinition,
) -> bool {
    (principal.subject_allowlist.is_none()
        || elephant_knowledge_views::has_complete_definition_subject_contract(definition))
        && state
            .authz
            .authorize_record_read(principal, definition, "read:entity")
            .is_ok()
}

async fn load_article_navigation(
    state: &KnowledgeViewsState,
    principal: &AuthPrincipal,
    view: &KnowledgeView,
) -> Result<Option<KnowledgeViewNavigation>, elephant_storage::StorageError> {
    let Ok(definition_id) = view.root_record.parse::<RecordId>() else {
        return Ok(None);
    };
    Ok(state
        .definitions
        .get(&view.base.space_id, &definition_id)
        .await?
        .filter(|definition| can_read_definition(state, principal, definition))
        .and_then(|definition| definition.navigation))
}

async fn load_wiki_navigation(
    state: &KnowledgeViewsState,
    principal: &AuthPrincipal,
    space_id: &str,
) -> Result<
    std::collections::BTreeMap<String, KnowledgeViewNavigation>,
    elephant_storage::StorageError,
> {
    let mut navigation = std::collections::BTreeMap::new();
    let mut cursor = None;
    loop {
        let page = state
            .definitions
            .list_page(space_id, 250, cursor.as_ref())
            .await?;
        if page.is_empty() {
            break;
        }
        let last_page = page.len() < 250;
        cursor = page.last().map(|definition| definition.base.id.clone());
        for definition in page {
            if !can_read_definition(state, principal, &definition) {
                continue;
            }
            if let Some(item) = definition.navigation {
                navigation.insert(definition.base.id.to_string(), item);
            }
        }
        if last_page {
            break;
        }
    }
    Ok(navigation)
}

fn render_related_articles(views: &[KnowledgeView]) -> String {
    if views.is_empty() {
        return String::new();
    }
    let mut html = "<section><h2>Related articles</h2><ul>".to_string();
    for view in views {
        let _ = write!(
            html,
            "<li><a href=\"/wiki/{}\">{}</a></li>",
            escape_html(&view.base.id.to_string()),
            escape_html(&view.title),
        );
    }
    html.push_str("</ul></section>");
    html
}

fn article_lead(revision: &KnowledgeViewRevision) -> Option<String> {
    revision
        .blocks
        .iter()
        .find(|block| block.kind == ViewBlockKind::Overview)
        .and_then(|block| block.statements.first())
        .map(|statement| plain_statement_text(&statement.text))
        .filter(|text| !text.is_empty())
}

const fn is_semantic_block(kind: ViewBlockKind) -> bool {
    matches!(
        kind,
        ViewBlockKind::Overview
            | ViewBlockKind::CurrentFacts
            | ViewBlockKind::Relationships
            | ViewBlockKind::Timeline
            | ViewBlockKind::Members
            | ViewBlockKind::Conflicts
    )
}

fn statement_is_list_item(value: &str) -> bool {
    value.trim_start().starts_with("- ")
}

fn plain_statement_text(value: &str) -> String {
    let value = value.trim();
    let value = value.strip_prefix("- ").unwrap_or(value);
    let value = value.replace("**", "");
    let mut output = String::with_capacity(value.len());
    let mut remainder = value.as_str();
    while let Some(start) = remainder.find("[^c") {
        output.push_str(&remainder[..start]);
        let citation = &remainder[start..];
        let Some(end) = citation.find(']') else {
            output.push_str(citation);
            remainder = "";
            break;
        };
        remainder = &citation[end + 1..];
    }
    output.push_str(remainder);
    output.trim().to_string()
}

fn not_found_html(message: &str) -> (StatusCode, Html<String>) {
    (
        StatusCode::NOT_FOUND,
        Html(page_shell(
            "Not found",
            &format!("<h1>Not found</h1><p>{}</p>", escape_html(message)),
        )),
    )
}

const fn kind_name(kind: KnowledgeViewKind) -> &'static str {
    match kind {
        KnowledgeViewKind::Article => "Article",
        KnowledgeViewKind::Entity => "Entity",
        KnowledgeViewKind::Source => "Source",
        KnowledgeViewKind::Topic => "Topic",
        KnowledgeViewKind::Community => "Community",
        KnowledgeViewKind::Query => "Saved query",
    }
}

const fn state_name(state: elephant_types::records::ProjectionState) -> &'static str {
    use elephant_types::records::ProjectionState;
    match state {
        ProjectionState::Current => "current",
        ProjectionState::Dirty => "dirty",
        ProjectionState::Rebuilding => "rebuilding",
        ProjectionState::Degraded => "degraded",
        ProjectionState::Failed => "failed",
    }
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// Returns Living Wiki JSON and browser routes.
pub fn knowledge_view_routes() -> Router<Arc<KnowledgeViewsState>> {
    Router::new()
        .route("/v1/knowledge-views", get(list_views))
        .route("/v1/knowledge-views/{id}", get(get_view))
        .route("/wiki", get(browse_index))
        .route("/wiki/{id}", get(browse_view))
        .route("/evidence", get(browse_evidence_index))
        .route("/evidence/{id}", get(browse_evidence_view))
        .route("/inspect/knowledge-views", get(browse_inspector_index))
        .route("/inspect/knowledge-views/{id}", get(browse_inspector_view))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use elephant_types::records::ViewBlockKind;
    use std::collections::HashSet;

    use super::{escape_html, is_semantic_block, plain_statement_text};

    #[tokio::test]
    async fn source_http_document_requires_raw_scope() {
        use super::*;
        use elephant_storage::{Repository, SourceRepository};
        use elephant_types::records::{BaseFields, Source, SourceType};
        let storage = StorageClient::new_embedded().await.expect("storage");
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "space".to_string(),
                "test".to_string(),
            ),
            SourceType::Manual,
            "sha256:test".to_string(),
        );
        let source_id = source.base.id.clone();
        SourceRepository::new(storage.clone())
            .create(source)
            .await
            .expect("source");
        let compiled = SourceViewCompiler::new(storage.clone())
            .compile("space", &source_id, SourceViewLimits::default(), false)
            .await
            .expect("compile");
        let state = KnowledgeViewsState::new(storage, Arc::new(crate::authz::AuthzService::new()));
        let mut principal = AuthPrincipal {
            space_id: Some("space".to_string()),
            scopes: vec!["read:source".to_string()],
            clearances: vec![elephant_policy::Clearance {
                policy_id: "default".to_string(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            ..Default::default()
        };
        assert_eq!(
            load_document(&state, &principal, &compiled.view.base.id.to_string())
                .await
                .expect_err("raw denied")
                .0,
            StatusCode::FORBIDDEN
        );
        principal.scopes.push("read:doc:raw".to_string());
        assert!(
            load_document(&state, &principal, &compiled.view.base.id.to_string())
                .await
                .is_ok()
        );
        let mut legacy = compiled.view;
        legacy.kind = KnowledgeViewKind::Article;
        legacy.base.provenance.extractor = Some(elephant_types::records::base::Extractor {
            name: Some("elephant.llm_article".to_string()),
            version: Some("14".to_string()),
            model: None,
        });
        principal.scopes.push("read:entity".to_string());
        assert!(principal.subject_allowlist.is_none());
        assert!(authorize_view(&state, &principal, &legacy).is_err());
    }

    #[tokio::test]
    async fn navigation_authorizes_complete_definition_and_rejects_legacy_scope() {
        use super::*;
        use elephant_types::records::base::Extractor;
        use elephant_types::records::{BaseFields, KnowledgeViewSelector, SubjectsState};
        use elephant_types::Link;
        let storage = StorageClient::new_embedded().await.expect("storage");
        let root = RecordId::new_v4(TableName::Entity);
        let other = RecordId::new_v4(TableName::Entity);
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::KnowledgeViewDefinition),
            "space".to_string(),
            "elephant.wiki_planner".to_string(),
        );
        base.subjects = vec![Link::new(root.clone()), Link::new(other.clone())];
        base.subjects_state = SubjectsState::Complete;
        base.provenance.extractor = Some(Extractor {
            name: Some("elephant.wiki_planner".to_string()),
            version: Some("10".to_string()),
            model: None,
        });
        let definition = KnowledgeViewDefinition {
            base,
            title: "Allowed".to_string(),
            publication_status: KnowledgeViewPublicationStatus::Published,
            related_articles: Vec::new(),
            navigation: Some(KnowledgeViewNavigation {
                wiki_title: "Secret project".to_string(),
                wiki_purpose: "Private inventory".to_string(),
                section_id: "private".to_string(),
                section_title: "Secret project".to_string(),
                section_description: "Other subject".to_string(),
                section_order: 0,
                article_order: 0,
                article_importance: 5,
                page_role: "core".to_string(),
                featured: true,
            }),
            selector: KnowledgeViewSelector::Topic {
                topic: Link::new(root.clone()),
                relation_types: Vec::new(),
                max_hops: 1,
                max_members: 10,
            },
        };
        let state = KnowledgeViewsState::new(storage, Arc::new(crate::authz::AuthzService::new()));
        let mut principal = AuthPrincipal {
            space_id: Some("space".to_string()),
            scopes: vec!["read:entity".to_string()],
            subject_allowlist: Some(vec![root.clone()]),
            clearances: vec![elephant_policy::Clearance {
                policy_id: "default".to_string(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            ..Default::default()
        };
        state
            .definitions
            .upsert(definition.clone())
            .await
            .expect("definition");
        assert!(load_wiki_navigation(&state, &principal, "space")
            .await
            .expect("navigation")
            .is_empty());
        principal.subject_allowlist = Some(vec![root, other]);
        assert_eq!(
            load_wiki_navigation(&state, &principal, "space")
                .await
                .expect("navigation")
                .len(),
            1
        );
        let mut legacy = definition;
        legacy
            .base
            .provenance
            .extractor
            .as_mut()
            .expect("extractor")
            .version = Some("9".to_string());
        state.definitions.upsert(legacy).await.expect("legacy");
        assert!(load_wiki_navigation(&state, &principal, "space")
            .await
            .expect("navigation")
            .is_empty());
    }

    #[test]
    fn browser_and_json_routes_reject_missing_or_blank_space() {
        for space_id in [None, Some(String::new()), Some("  ".to_string())] {
            let principal = crate::auth::Principal {
                space_id,
                ..Default::default()
            };
            assert!(super::require_space(&principal).is_err());
            assert!(super::require_space_html(&principal).is_err());
        }
    }

    #[test]
    fn browser_escapes_stored_markdown_before_rendering() {
        assert_eq!(
            escape_html("<script>alert('x')</script>"),
            "&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt;"
        );
    }

    #[test]
    fn browser_renders_claim_text_without_markdown_receipt_marks() {
        assert_eq!(
            plain_statement_text("- **Eve** attends Northbank Academy.[^c1] [^c22]"),
            "Eve attends Northbank Academy."
        );
        assert_eq!(
            plain_statement_text("- **Lakeside** is home.[^c1]"),
            "Lakeside is home."
        );
    }

    #[test]
    fn semantic_surface_hides_compiler_and_lineage_blocks() {
        assert!(is_semantic_block(ViewBlockKind::Relationships));
        assert!(!is_semantic_block(ViewBlockKind::SelectionReceipt));
        assert!(!is_semantic_block(ViewBlockKind::SourceLineage));
        assert!(!is_semantic_block(ViewBlockKind::EvidenceStatus));
    }
}
