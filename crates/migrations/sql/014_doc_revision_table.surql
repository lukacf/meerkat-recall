-- Ensure the active doc_revision table exists under applied schema and keeps
-- nested renderer/manifest objects intact for multimodal revisions.

DEFINE TABLE doc_revision SCHEMALESS;
DEFINE FIELD renderer ON TABLE doc_revision TYPE object FLEXIBLE;
DEFINE FIELD manifest ON TABLE doc_revision TYPE option<object> FLEXIBLE;
