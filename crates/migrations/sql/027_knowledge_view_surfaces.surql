-- Separate LLM-authored wiki articles from source evidence and deterministic
-- graph projections. Existing graph definitions are intentionally backfilled
-- as inspector drafts so a migration never publishes heuristic prose.

UPDATE knowledge_view SET surface = 'evidence', publication_status = 'published'
WHERE kind = 'source' AND (surface IS NONE OR publication_status IS NONE);

UPDATE knowledge_view SET surface = 'inspector', publication_status = 'published'
WHERE kind = 'entity' AND (surface IS NONE OR publication_status IS NONE);

UPDATE knowledge_view SET surface = 'wiki', publication_status = 'draft'
WHERE kind = 'article' AND (surface IS NONE OR publication_status IS NONE);

UPDATE knowledge_view SET surface = 'inspector', publication_status = 'draft'
WHERE kind != 'source' AND kind != 'entity' AND kind != 'article'
    AND (surface IS NONE OR publication_status IS NONE);

UPDATE knowledge_view_definition SET publication_status = 'draft'
WHERE publication_status IS NONE;

DEFINE INDEX IF NOT EXISTS knowledge_view_surface_publication ON TABLE knowledge_view FIELDS space_id, surface, publication_status, deleted_at, updated_at;
