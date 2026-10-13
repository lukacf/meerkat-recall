-- 019: index the media_asset and doc_revision scan fields.
--
-- media_asset rows carry inline base64 payloads (hundreds of KB each in
-- prod), and doc_revision rows carry full document texts. Neither table had
-- ANY index, so the space-scoped list scans and the pipeline's
-- find_by_doc/find_by_status reads deserialized every heavy row in the table
-- on the RocksDB engine (issue #34). Equality on these fields is what every
-- one of those queries filters on, so single-field indexes are the fix.
DEFINE INDEX IF NOT EXISTS media_asset_space ON TABLE media_asset FIELDS space_id;
DEFINE INDEX IF NOT EXISTS doc_revision_doc ON TABLE doc_revision FIELDS doc;
DEFINE INDEX IF NOT EXISTS doc_revision_status ON TABLE doc_revision FIELDS status;
DEFINE INDEX IF NOT EXISTS doc_revision_space ON TABLE doc_revision FIELDS space_id;
