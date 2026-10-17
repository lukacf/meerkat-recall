//! The actual CLI receipt reader preserves line, item, byte and uniqueness bounds.
use super::{InitialBindingInput, InitialReceipt, MAX_INITIAL_BINDING_BATCH_RECEIPTS};
use elephant_types::publication::{OperationReceipt, OperationState};
use elephant_types::{RecordId, TableName};
use std::collections::BTreeMap;
use std::io::Cursor;

fn receipt() -> InitialReceipt {
    let source_id = RecordId::new_v4(TableName::Source);
    InitialReceipt {
        source_space_id: "original".into(),
        space_id: "target".into(),
        external_source_id: "fixture".into(),
        operation: OperationReceipt {
            space_id: "target".into(),
            operation_id: "operation".into(),
            source_id: source_id.clone(),
            source_generation: "generation".into(),
            desired_revision: Some("revision".into()),
            published_revision: Some("revision".into()),
            state: OperationState::Completed,
            accepted_at: chrono::Utc::now(),
            completed_at: None,
            error_code: None,
        },
        source_id,
        accepted_revision: "revision".into(),
        accepted_request_fingerprint: "a".repeat(64),
        document_mapping_sha256: "b".repeat(64),
        document_content_sha256: BTreeMap::new(),
        manifest_digest: "c".repeat(64),
        original_snapshot_sha256: "d".repeat(64),
        commit_id: "commit".into(),
    }
}

#[test]
fn receipt_input_crosses_256_boundary_in_original_order() -> anyhow::Result<()> {
    let originals: Vec<_> = (0..=MAX_INITIAL_BINDING_BATCH_RECEIPTS)
        .map(|_| receipt())
        .collect();
    let mut bytes = Vec::new();
    for original in &originals {
        serde_json::to_writer(&mut bytes, original)?;
        bytes.push(b'\n');
    }
    let mut source = Cursor::new(bytes);
    let mut input = InitialBindingInput::default();
    let first = input.read_batch(&mut source, originals.len())?;
    let second = input.read_batch(&mut source, originals.len())?;
    assert_eq!(first.len(), MAX_INITIAL_BINDING_BATCH_RECEIPTS);
    assert_eq!(second.len(), 1);
    assert!(input.read_batch(&mut source, originals.len())?.is_empty());
    assert_eq!(
        first.into_iter().chain(second).collect::<Vec<_>>(),
        originals
    );
    Ok(())
}

#[test]
fn receipt_input_flushes_by_bytes_and_accepts_exact_original_line_limit() -> anyhow::Result<()> {
    let originals = [receipt(), receipt(), receipt()];
    let mut bytes = Vec::new();
    for original in &originals {
        let begin = bytes.len();
        serde_json::to_writer(&mut bytes, original)?;
        bytes.resize(begin + 8 * 1024 * 1024 - 1, b' ');
        bytes.push(b'\n');
    }
    let mut source = Cursor::new(bytes);
    let mut input = InitialBindingInput::default();
    assert_eq!(input.read_batch(&mut source, 3)?, originals[..2]);
    assert_eq!(input.read_batch(&mut source, 3)?, originals[2..]);
    assert!(input.read_batch(&mut source, 3)?.is_empty());
    Ok(())
}

#[test]
fn receipt_input_rejects_duplicate_extra_incomplete_and_oversized_lines() -> anyhow::Result<()> {
    let original = receipt();
    let encoded = serde_json::to_vec(&original)?;
    let mut duplicate = encoded.clone();
    duplicate.push(b'\n');
    duplicate.extend_from_slice(&encoded);
    duplicate.push(b'\n');
    assert!(InitialBindingInput::default()
        .read_batch(&mut Cursor::new(duplicate), 2)
        .is_err());
    let mut complete = encoded.clone();
    complete.push(b'\n');
    assert!(InitialBindingInput::default()
        .read_batch(&mut Cursor::new(complete), 0)
        .is_err());
    assert!(InitialBindingInput::default()
        .read_batch(&mut Cursor::new(encoded.clone()), 1)
        .is_err());
    let mut oversized = encoded;
    oversized.resize(8 * 1024 * 1024, b' ');
    oversized.push(b'\n');
    assert!(InitialBindingInput::default()
        .read_batch(&mut Cursor::new(oversized), 1)
        .is_err());
    Ok(())
}
