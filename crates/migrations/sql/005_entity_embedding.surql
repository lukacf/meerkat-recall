-- Elephant Core Schema: Entity Embedding Support
-- Migration: 005_entity_embedding
-- This adds vector embedding support to entities for semantic search.
-- Per spec Section 9: Vector candidate generation requires entity embeddings.

-- =============================================================================
-- Entity Embedding Field
-- =============================================================================

-- Entity name embedding for semantic search
-- Uses the same 3072 dimensions as doc_chunk embeddings (text-embedding-3-large)
DEFINE FIELD name_embedding ON TABLE entity TYPE option<array<float>> ASSERT $value IS NONE OR array::len($value) = 3072;
DEFINE FIELD name_embedding_model ON TABLE entity TYPE option<string>;

-- =============================================================================
-- Entity Vector Search Index
-- =============================================================================

-- HNSW index for efficient cosine similarity search on entity name embeddings
DEFINE INDEX entity_name_vec ON TABLE entity FIELDS name_embedding HNSW DIMENSION 3072 DIST COSINE;
