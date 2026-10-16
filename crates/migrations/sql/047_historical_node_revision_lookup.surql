-- Complete bounded revision membership for historical vector coverage.
-- Index only revision: a foreign-space row referencing the same revision must
-- remain visible to the strict lineage validator, never silently filtered.
DEFINE INDEX IF NOT EXISTS historical_node_revision_lookup ON doc_node FIELDS revision;
