-- 023: reverse dependency edges for indexed Living Wiki invalidation.
--
-- The complete current edge set is replaced atomically with each publication.
-- Historical dependency receipts remain in immutable knowledge_view_revision.

DEFINE TABLE IF NOT EXISTS knowledge_view_dependency SCHEMALESS;

DEFINE INDEX IF NOT EXISTS knowledge_view_dependency_unique ON TABLE knowledge_view_dependency FIELDS space_id, view, dependency_record UNIQUE;
DEFINE INDEX IF NOT EXISTS knowledge_view_dependency_reverse ON TABLE knowledge_view_dependency FIELDS space_id, dependency_record;
DEFINE INDEX IF NOT EXISTS knowledge_view_dependency_view ON TABLE knowledge_view_dependency FIELDS space_id, view;
