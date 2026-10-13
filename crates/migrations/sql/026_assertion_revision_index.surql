-- Source evidence pages resolve extracted assertions by immutable document
-- revision. Without this index, a single source read scans the full assertion
-- corpus before semantic claims can be linked back to their evidence.

DEFINE INDEX IF NOT EXISTS assertion_revision_lookup ON TABLE assertion FIELDS space_id, evidence.revision, deleted_at, created_at;
DEFINE INDEX IF NOT EXISTS knowledge_view_kind_lookup ON TABLE knowledge_view FIELDS space_id, kind, deleted_at, updated_at;
