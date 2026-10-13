-- 021: graph-native derived knowledge projections.
--
-- A knowledge view is a stable, security-scoped definition. Each publication
-- creates an immutable revision carrying its complete citation, dependency,
-- freshness, compiler, and validation receipt. Generated text is a projection
-- only and is never a source or assertion.

DEFINE TABLE IF NOT EXISTS knowledge_view SCHEMALESS;
DEFINE TABLE IF NOT EXISTS knowledge_view_revision SCHEMALESS;

DEFINE INDEX IF NOT EXISTS knowledge_view_space_key ON TABLE knowledge_view FIELDS space_id, key UNIQUE;
DEFINE INDEX IF NOT EXISTS knowledge_view_root ON TABLE knowledge_view FIELDS space_id, root_record;
DEFINE INDEX IF NOT EXISTS knowledge_view_state ON TABLE knowledge_view FIELDS space_id, state;
DEFINE INDEX IF NOT EXISTS knowledge_view_revision_parent ON TABLE knowledge_view_revision FIELDS space_id, view_id, revision UNIQUE;
DEFINE INDEX IF NOT EXISTS knowledge_view_revision_compiled_at ON TABLE knowledge_view_revision FIELDS space_id, compiled_at;
