-- Keep hot queue reads independent of accumulated terminal work history.
-- Idempotency remains non-unique: existing databases can contain duplicate
-- keys, and enforcing uniqueness requires a separate reconciliation policy.
DEFINE INDEX IF NOT EXISTS work_item_status ON TABLE work_item FIELDS status;
DEFINE INDEX IF NOT EXISTS work_item_status_kind ON TABLE work_item FIELDS status, kind;
DEFINE INDEX IF NOT EXISTS work_item_updated ON TABLE work_item FIELDS updated_at;
DEFINE INDEX IF NOT EXISTS work_item_space_status_kind ON TABLE work_item FIELDS space_id, status, kind;
DEFINE INDEX IF NOT EXISTS work_item_space_idempotency ON TABLE work_item FIELDS space_id, idempotency_key;
DEFINE INDEX IF NOT EXISTS work_item_source ON TABLE work_item FIELDS payload.source_id;
DEFINE INDEX IF NOT EXISTS work_item_space_source_status_kind ON TABLE work_item FIELDS space_id, payload.source_id, status, kind;
DEFINE INDEX IF NOT EXISTS work_item_space_kind ON TABLE work_item FIELDS space_id, kind;

-- Execution receipts and scheduler dependency joins are point lookups by
-- work item. Keep them indexed as the corresponding history grows.
DEFINE INDEX IF NOT EXISTS work_run_item ON TABLE work_run FIELDS work_item;
DEFINE INDEX IF NOT EXISTS work_run_completed ON TABLE work_run FIELDS completed_at;
DEFINE INDEX IF NOT EXISTS work_edge_to_item ON TABLE work_edge FIELDS to_item;
DEFINE INDEX IF NOT EXISTS work_edge_from_item ON TABLE work_edge FIELDS from_item;
