-- Migration 007: Drop doc_chunk table
-- DocChunk has been replaced by DocNode with embedding fields.
-- All chunking now goes through doc_node records.
REMOVE TABLE IF EXISTS doc_chunk;
