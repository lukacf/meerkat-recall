-- Composite indexes for the bounded reads performed by Living Wiki compilers.
-- The single-field indexes predate graph-scale materialization and still leave
-- SurrealDB filtering and sorting whole record families for each root.

DEFINE INDEX IF NOT EXISTS knowledge_view_doc_source ON TABLE doc FIELDS space_id, source, deleted_at, created_at;
DEFINE INDEX IF NOT EXISTS knowledge_view_doc_revision_doc ON TABLE doc_revision FIELDS doc, deleted_at, created_at;
DEFINE INDEX IF NOT EXISTS knowledge_view_rel_outgoing ON TABLE rel FIELDS space_id, `in`, deleted_at;
DEFINE INDEX IF NOT EXISTS knowledge_view_rel_incoming ON TABLE rel FIELDS space_id, `out`, deleted_at;
DEFINE INDEX IF NOT EXISTS knowledge_view_attribute_subject ON TABLE attribute FIELDS space_id, subject, deleted_at, attr_type;
DEFINE INDEX IF NOT EXISTS knowledge_view_event_subjects ON TABLE event FIELDS space_id, subjects, deleted_at, time_start;
DEFINE INDEX IF NOT EXISTS knowledge_view_truth_subject ON TABLE truth_slot FIELDS space_id, subject_entity, deleted_at, computed_at;
DEFINE INDEX IF NOT EXISTS knowledge_view_freshness_target ON TABLE freshness FIELDS space_id, target, deleted_at, computed_at;
