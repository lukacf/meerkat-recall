-- 024: governed typed definitions for graph-derived Living Wiki pages.

DEFINE TABLE IF NOT EXISTS knowledge_view_definition SCHEMALESS;

DEFINE INDEX IF NOT EXISTS knowledge_view_definition_kind ON TABLE knowledge_view_definition FIELDS space_id, selector.type;
DEFINE INDEX IF NOT EXISTS knowledge_view_definition_updated ON TABLE knowledge_view_definition FIELDS space_id, updated_at;
