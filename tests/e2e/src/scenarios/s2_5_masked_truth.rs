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
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::Doc;
use elephant_types::RecordId;

use elephant_api::auth::Principal as AuthPrincipal;
use elephant_api::redaction::redact_doc;

#[tokio::test]
#[ignore]
async fn scenario_s2_5_masked_truth_redaction() -> anyhow::Result<()> {
    // Mock a metadata-only principal.
    let principal = AuthPrincipal {
        subject: "metadata_reader".to_string(),
        space_id: Some("Space_X".to_string()),
        scopes: vec!["read:doc:metadata".to_string()],
        security_level: 2,
        ..Default::default()
    };

    // Create a Doc D1: level 2, text "Top Secret"
    let id = RecordId::new_v4(TableName::Doc);
    let mut base = BaseFields::new(id, "Space_X".to_string(), "P_sys".to_string());
    base.security = Security {
        policy_id: "default".to_string(),
        level: 2,
        labels: vec![],
        handling: vec![],
    };

    let doc = Doc {
        base,
        title: "Confidential Report".to_string(),
        text: "This is a secret message.".to_string(),
        sidecar_text: None,
        source: None,
        media_attachments: Vec::new(),
        rendition_key: "r1".to_string(),
        metadata: serde_json::Value::Null,
    };

    // 1. Redact the doc for the metadata-only principal.
    let envelope = redact_doc(&principal, doc);

    // 2. Verify Invariant: Text is gone, redactions marked
    assert!(
        envelope.data.text.is_empty(),
        "Doc text should have been redacted for principal without read:doc:raw scope"
    );
    assert!(
        envelope.redactions.contains(&"doc.text".to_string()),
        "Envelope should list 'doc.text' in redactions"
    );

    Ok(())
}
