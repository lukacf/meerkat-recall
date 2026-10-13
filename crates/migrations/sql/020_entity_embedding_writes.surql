-- 020: make entity embeddings writable (semantic search, de-heuristic pass).
--
-- Migration 005 defined entity.name_embedding with a hard 3072-dim ASSERT
-- plus a fixed-dimension HNSW index, sized for text-embedding-3-large — but
-- nothing ever wrote the field, so resolution's vector candidate generation
-- and any semantic ranking were silent no-ops. The maintenance sweep now
-- writes embeddings using whichever provider the space is configured with
-- (hashed/fastembed/openai), whose dimensions vary, so:
--   * drop the 3072 ASSERT (dimension agreement is enforced at query time
--     with an array::len() guard instead),
--   * drop the fixed-dimension HNSW index — inserts with other dimensions
--     would fail against it, and vector_search_entities ranks with
--     vector::similarity::cosine over the space's entities directly (entity
--     counts per space are small; the index was never used by that query),
--   * add name_embedding_text_hash so the sweep can skip entities whose
--     embedded text (name + aliases + summary) is unchanged.
DEFINE FIELD OVERWRITE name_embedding ON TABLE entity TYPE option<array<float>>;
DEFINE FIELD OVERWRITE name_embedding_model ON TABLE entity TYPE option<string>;
DEFINE FIELD IF NOT EXISTS name_embedding_text_hash ON TABLE entity TYPE option<string>;
REMOVE INDEX IF EXISTS entity_name_vec ON TABLE entity;
