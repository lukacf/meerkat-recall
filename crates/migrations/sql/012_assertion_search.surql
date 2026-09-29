-- Assertion lexical search indexes
-- Migration: 012_assertion_search

DEFINE INDEX assertion_subject_text_search ON TABLE assertion FIELDS subject_text SEARCH ANALYZER simple BM25;
DEFINE INDEX assertion_predicate_raw_search ON TABLE assertion FIELDS predicate_raw SEARCH ANALYZER simple BM25;
DEFINE INDEX assertion_object_text_search ON TABLE assertion FIELDS object_text SEARCH ANALYZER simple BM25;
