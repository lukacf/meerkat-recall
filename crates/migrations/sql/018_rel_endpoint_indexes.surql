-- 018: index the relationship endpoint fields.
--
-- get_entity_context_bundle and the graph reads fetch an entity's relations
-- with `SELECT ... FROM rel WHERE `in` = $id` and `WHERE `out` = $id`. The
-- rel table had indexes on space_id/type/status but NONE on `in`/`out`, so
-- every entity read full-scanned the whole rel table twice. Cheap in the old
-- in-memory engine, seconds-slow on the RocksDB engine (issue #31). Equality
-- on the endpoint is highly selective (an entity has few edges), so a
-- single-field index on each endpoint is the fix.
DEFINE INDEX IF NOT EXISTS rel_in ON TABLE rel FIELDS `in`;
DEFINE INDEX IF NOT EXISTS rel_out ON TABLE rel FIELDS `out`;
