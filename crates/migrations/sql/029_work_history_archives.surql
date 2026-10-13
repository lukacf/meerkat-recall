-- Cold scheduler history retains original public identities and full records.
DEFINE TABLE IF NOT EXISTS work_item_archive SCHEMALESS;
DEFINE TABLE IF NOT EXISTS work_run_archive SCHEMALESS;
DEFINE INDEX IF NOT EXISTS work_item_archive_space_original ON work_item_archive FIELDS space_id, original_id;
DEFINE INDEX IF NOT EXISTS work_item_archive_space_key ON work_item_archive FIELDS space_id, record.idempotency_key;
DEFINE INDEX IF NOT EXISTS work_item_archive_space_status_kind ON work_item_archive FIELDS space_id, record.status, record.kind;
DEFINE INDEX IF NOT EXISTS work_item_archive_source ON work_item_archive FIELDS record.payload.source_id;
DEFINE INDEX IF NOT EXISTS work_item_archive_original ON work_item_archive FIELDS original_id;
DEFINE INDEX IF NOT EXISTS work_run_archive_space_original ON work_run_archive FIELDS space_id, original_id;
DEFINE INDEX IF NOT EXISTS work_run_archive_parent ON work_run_archive FIELDS record.work_item;
DEFINE INDEX IF NOT EXISTS work_item_archive_space_source_status_kind ON work_item_archive FIELDS space_id, record.payload.source_id, record.status, record.kind;
DEFINE INDEX IF NOT EXISTS work_item_archive_revision ON work_item_archive FIELDS record.payload.revision_id;
DEFINE INDEX IF NOT EXISTS work_item_archive_doc ON work_item_archive FIELDS record.payload.doc_id;
DEFINE INDEX IF NOT EXISTS work_item_revision ON work_item FIELDS payload.revision_id;
DEFINE INDEX IF NOT EXISTS work_item_doc ON work_item FIELDS payload.doc_id;
DEFINE INDEX IF NOT EXISTS work_item_archive_space_kind_grant ON work_item_archive FIELDS space_id, record.kind, record.payload.authority.grant_id;
