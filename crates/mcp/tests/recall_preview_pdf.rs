//! Frozen synthetic PDFs through the exact active parser and real source intake.
//! No worker, provider, network service, generated parser answer or skip-auth.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use elephant_mcp::{
    handlers::{GetDocHandler, IngestFileHandler},
    ToolContext, ToolHandler,
};
use elephant_policy::{Clearance, Principal};
use elephant_storage::{
    AssertionRepository, DocRepository, Repository, SourcePublicationRepository, SourceRepository,
    SpaceConfigRepository, StorageClient, WorkItemRepository,
};
use elephant_types::publication::{OperationReceipt, OperationState, PublicationFence};
use elephant_types::records::{base::BaseFields, Doc, SpaceConfig};
use elephant_types::{RecordId, TableName};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const CORPUS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/recall_preview_semantics/pdf_acceptance"
);
// Freeze is explicit: generator --write cannot silently update this acceptance pin.
const MANIFEST_SHA256: &str = "3e1e107671826ee4cadcf8ba2df8ded0fa68c6b005237cc87e66269d0b491f1a";
const SPACE: &str = "pdf-acceptance";

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn manifest() -> TestResult<Value> {
    let bytes = std::fs::read(Path::new(CORPUS).join("manifest.json"))?;
    assert_eq!(digest(&bytes), MANIFEST_SHA256, "corpus freeze changed");
    Ok(serde_json::from_slice(&bytes)?)
}

fn expectations() -> TestResult<Value> {
    let bytes = std::fs::read(Path::new(CORPUS).join("expected.json"))?;
    assert_eq!(
        digest(&bytes),
        manifest()?["expected_sha256"].as_str().unwrap()
    );
    Ok(serde_json::from_slice(&bytes)?)
}

fn case(id: &str) -> TestResult<Value> {
    Ok(expectations()?["cases"]
        .as_array()
        .expect("frozen case list")
        .iter()
        .find(|case| case["id"] == id)
        .expect("frozen case ID")
        .clone())
}

fn fixture(id: &str) -> TestResult<(PathBuf, Vec<u8>)> {
    let manifest = manifest()?;
    let entry = manifest["files"]
        .as_array()
        .expect("frozen file list")
        .iter()
        .find(|entry| entry["id"] == id)
        .expect("frozen file ID");
    let relative = entry["file"].as_str().expect("relative fixture path");
    assert_eq!(relative, format!("fixtures/{id}.pdf"));
    let path = Path::new(CORPUS).join(relative);
    let bytes = std::fs::read(&path)?;
    assert_eq!(bytes.len() as u64, entry["bytes"].as_u64().unwrap());
    assert!(bytes.len() <= 131_072, "bounded synthetic fixture");
    assert_eq!(
        digest(&bytes),
        entry["sha256"].as_str().unwrap(),
        "fixture {id}"
    );
    Ok((path, bytes))
}

fn expected_text(case: &Value) -> String {
    case["lines"]
        .as_array()
        .expect("literal expected lines")
        .iter()
        .map(|line| line.as_str().expect("literal expected line"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn assert_text(case: &Value, text: &str) {
    assert_eq!(
        text.replace("\r\n", "\n").trim(),
        expected_text(case),
        "exact passages/order for {}",
        case["id"]
    );
    for forbidden in case["forbidden"].as_array().unwrap() {
        assert!(
            !text.contains(forbidden.as_str().unwrap()),
            "unexpected metadata/placeholder/hidden text in {}",
            case["id"]
        );
    }
}

fn parse_case(id: &str) -> TestResult {
    let case = case(id)?;
    let (_, bytes) = fixture(id)?;
    // Exact same final API as the active application/pdf dispatch in docs.rs.
    let result = pdf_inspector::extractor::extract_text_mem(&bytes);
    if case["outcome"] == "text" {
        assert_text(&case, &result?);
    } else {
        let error = result.expect_err("malformed document cannot return successful partial text");
        for fragment in case["error_contains"].as_array().unwrap() {
            assert!(
                error.to_string().contains(fragment.as_str().unwrap()),
                "case {id}: {error}"
            );
        }
    }
    Ok(())
}

macro_rules! parser_case {
    ($name:ident, $id:literal) => {
        #[test]
        fn $name() -> TestResult {
            parse_case($id)
        }
    };
}

parser_case!(pdf_winansi, "winansi");
parser_case!(pdf_multilingual_tounicode, "multilingual");
parser_case!(pdf_page_order_and_literal_markup, "page_order");
parser_case!(pdf_nested_and_repeated_forms, "nested_forms");
parser_case!(pdf_inherited_form_resources, "inherited_forms");
parser_case!(pdf_acroform_field_values, "acroform");
parser_case!(pdf_existing_invisible_ocr_layer, "existing_ocr_layer");
parser_case!(pdf_image_only_is_empty, "image_only");
parser_case!(pdf_crop_visibility, "crop_visibility");
parser_case!(pdf_blank_page_is_empty, "blank_page");
parser_case!(pdf_missing_header_is_error, "not_pdf");
parser_case!(pdf_unfinished_catalog_is_error, "truncated");
parser_case!(pdf_corrupt_later_page_is_total_error, "broken_second_page");

#[test]
fn pdf_corpus_has_exactly_the_frozen_members() -> TestResult {
    let manifest = manifest()?;
    let expected = expectations()?;
    let mut declared: Vec<_> = manifest["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|file| file["id"].as_str().unwrap())
        .collect();
    let mut answers: Vec<_> = expected["cases"]
        .as_array()
        .unwrap()
        .iter()
        .map(|case| case["id"].as_str().unwrap())
        .collect();
    declared.sort_unstable();
    answers.sort_unstable();
    assert_eq!(declared.len(), 13);
    assert_eq!(declared, answers);
    assert_eq!(
        declared
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        13
    );
    let mut actual: Vec<_> = std::fs::read_dir(Path::new(CORPUS).join("fixtures"))?
        .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
        .collect::<Result<_, _>>()?;
    actual.sort();
    assert_eq!(
        actual,
        declared
            .iter()
            .map(|id| format!("{id}.pdf"))
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[expect(
    clippy::default_trait_access,
    reason = "Keep the original test oracle and fixture lifetimes intact."
)]
async fn intake_context() -> TestResult<ToolContext> {
    let storage = StorageClient::new_embedded().await?;
    let mut config = serde_json::to_value(BaseFields::new(
        RecordId::new_v4(TableName::SpaceConfig),
        SPACE.into(),
        "pdf-acceptance-fixture".into(),
    ))?;
    config["embedding_provider"] = json!("not-dispatched");
    config["embedding_model"] = json!("no-model");
    config["embedding_dims"] = json!(8);
    config["processing"] = Value::Null;
    SpaceConfigRepository::new(storage.clone())
        .create(serde_json::from_value::<SpaceConfig>(config)?)
        .await?;
    let principal = Principal {
        space_id: SPACE.into(),
        scopes: [
            "read:source",
            "read:source:raw",
            "write:source",
            "read:doc:metadata",
            "read:doc:raw",
            "write:doc",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect(),
        clearances: vec![Clearance {
            policy_id: "default".into(),
            max_level: 0,
            labels: Default::default(),
            handling_allow: Default::default(),
        }],
        subject_allowlist: None,
        purpose: Default::default(),
    };
    Ok(ToolContext::new(storage, principal))
}

fn intake_request(id: &str, path: &Path) -> Value {
    json!({
        "path":path.to_str().expect("repository fixture path is UTF-8"),
        "source_type":"application/pdf", "title":format!("Synthetic PDF {id}"),
        "intake":{"revision_id":format!("pdf-{id}-v1"),"idempotency_key":format!("pdf-{id}-v1")}
    })
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    clippy::too_many_lines,
    reason = "Keep the original test oracle and fixture lifetimes intact."
)]
async fn pdf_active_intake_retains_exact_immutable_text_without_publication() -> TestResult {
    for case in expectations()?["cases"].as_array().unwrap() {
        if case["outcome"] != "text" {
            continue;
        }
        let id = case["id"].as_str().unwrap();
        let (path, bytes) = fixture(id)?;
        let parsed = pdf_inspector::extractor::extract_text_mem(&bytes)?;
        assert_text(case, &parsed);
        let context = intake_context().await?;
        assert!(!context.authz.is_skip_auth());
        let response = IngestFileHandler::new(context.storage.clone())
            .execute(&context, intake_request(id, &path))
            .await?;
        assert_eq!(response["ok"], true, "intake {id}: {response}");
        assert_text(
            case,
            response["data"]["data"]["text"]
                .as_str()
                .expect("raw staged text"),
        );
        let staged: Doc = serde_json::from_value(response["data"]["data"].clone())?;
        let operation: OperationReceipt = serde_json::from_value(response["operation"].clone())?;
        assert_eq!(operation.state, OperationState::Accepted);
        assert!(operation.published_revision.is_none());
        let repository = SourcePublicationRepository::new(context.storage.clone());
        let head = repository
            .head(SPACE, &operation.source_id)
            .await?
            .expect("accepted head");
        assert_eq!(head.generation, operation.source_generation);
        assert!(head.published_revision.is_none());
        let revision = repository
            .revision(
                SPACE,
                &operation.source_id,
                operation
                    .desired_revision
                    .as_deref()
                    .expect("accepted revision"),
            )
            .await?
            .expect("immutable accepted input");
        assert_eq!(revision.content_hash, digest(&bytes));
        let template = revision
            .source
            .manual_doc_template()?
            .expect("accepted PDF projection");
        assert_text(case, &template.text);
        assert_eq!(template.text, parsed, "intake preserves exact parser bytes");
        assert_eq!(
            staged.text, parsed,
            "raw preview preserves exact parser bytes"
        );
        assert_eq!(staged.base.security, revision.source.base.security);
        assert_eq!(staged.base.subjects, revision.source.base.subjects);
        assert_eq!(
            staged.base.subjects_state,
            revision.source.base.subjects_state
        );
        assert_eq!(revision.source.base.security.policy_id, "default");
        assert_eq!(revision.source.base.security.level, 0);
        assert!(revision.source.base.security.labels.is_empty());
        assert!(revision.source.base.security.handling.is_empty());
        assert_eq!(template.metadata["source_type"], "application/pdf");
        assert_eq!(template.metadata["content_type"], "text/plain");
        assert!(template.media_attachments.is_empty());
        // Acceptance persists a private recoverable document projection. It is
        // not a published rendering and must be unavailable to normal reads.
        let documents = DocRepository::new(context.storage.clone())
            .list(SPACE, 100, None)
            .await?;
        assert_eq!(
            documents.len(),
            1,
            "one pending projection for original intake"
        );
        assert_eq!(
            serde_json::to_value(&documents[0])?,
            serde_json::to_value(&staged)?,
            "persisted input must match the exact accepted preview"
        );
        let original: PublicationFence =
            serde_json::from_value(staged.metadata["_publication"].clone())?;
        assert_eq!(original.source_id, operation.source_id);
        assert_eq!(original.generation, operation.source_generation);
        assert_eq!(
            Some(original.revision_id.as_str()),
            operation.desired_revision.as_deref()
        );
        assert_eq!(original.operation_id, operation.operation_id);
        assert_eq!(
            staged.base.id,
            elephant_storage::source_intake::accepted_document_id(&original, &template.doc_id)?,
            "pending document identity binds this immutable accepted revision"
        );
        let reader =
            elephant_storage::read_eligibility::ReadEligibility::new(context.storage.clone());
        assert!(
            reader
                .project(documents[0].clone(), &|_| true)
                .await?
                .is_none(),
            "pending physical input is not publication, even with full clearance"
        );
        reader.verify_current().await?;
        let unavailable = GetDocHandler::new(context.storage.clone())
            .execute(&context, json!({"id": staged.base.id.to_string()}))
            .await
            .expect_err("normal get_doc cannot expose accepted-only input");
        assert!(
            unavailable.to_string().contains("current source support"),
            "{unavailable}"
        );
        assert!(
            elephant_storage::DocRevisionRepository::new(context.storage.clone())
                .find_by_doc(&staged.base.id)
                .await?
                .is_empty(),
            "no worker rendering was fabricated"
        );
        assert!(AssertionRepository::new(context.storage.clone())
            .list(SPACE, 100, None)
            .await?
            .is_empty());
        assert!(
            response["work_item_id"].is_string(),
            "accepted source must enqueue real work"
        );
        assert!(!WorkItemRepository::new(context.storage.clone())
            .list(SPACE, 100, None)
            .await?
            .is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn pdf_parse_failures_create_no_source_document_operation_or_work() -> TestResult {
    for case in expectations()?["cases"].as_array().unwrap() {
        if case["outcome"] != "error" {
            continue;
        }
        let id = case["id"].as_str().unwrap();
        let (path, _) = fixture(id)?;
        let context = intake_context().await?;
        let result = IngestFileHandler::new(context.storage.clone())
            .execute(&context, intake_request(id, &path))
            .await;
        assert!(
            matches!(result, Err(elephant_mcp::McpError::ExecutionError(ref message))
            if message.starts_with("Failed to extract PDF text:")),
            "case {id}: {result:?}"
        );
        assert!(SourceRepository::new(context.storage.clone())
            .list(SPACE, 100, None)
            .await?
            .is_empty());
        assert!(DocRepository::new(context.storage.clone())
            .list(SPACE, 100, None)
            .await?
            .is_empty());
        assert!(WorkItemRepository::new(context.storage.clone())
            .list(SPACE, 100, None)
            .await?
            .is_empty());
        assert!(SourcePublicationRepository::new(context.storage)
            .operation_by_idempotency_key(SPACE, &format!("pdf-{id}-v1"))
            .await?
            .is_none());
    }
    Ok(())
}
