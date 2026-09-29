-- Document lexical search indexes
-- Migration: 011_doc_search

DEFINE INDEX doc_title_search ON TABLE doc FIELDS title SEARCH ANALYZER simple BM25;
DEFINE INDEX doc_text_search ON TABLE doc FIELDS text SEARCH ANALYZER simple BM25;
