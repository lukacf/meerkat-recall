-- 022: durable remote-source freshness observations.
--
-- A source_watch owns mutable scheduling state. Each attempt emits an
-- immutable source_watch_check receipt. Neither table is source evidence;
-- they describe observation mechanics and freshness only.

DEFINE TABLE IF NOT EXISTS source_watch SCHEMALESS;
DEFINE TABLE IF NOT EXISTS source_watch_check SCHEMALESS;

DEFINE INDEX IF NOT EXISTS source_watch_source ON TABLE source_watch FIELDS space_id, source UNIQUE;
DEFINE INDEX IF NOT EXISTS source_watch_due ON TABLE source_watch FIELDS space_id, status, next_check_at;
DEFINE INDEX IF NOT EXISTS source_watch_check_watch ON TABLE source_watch_check FIELDS space_id, watch, completed_at;
DEFINE INDEX IF NOT EXISTS source_watch_check_source ON TABLE source_watch_check FIELDS space_id, source, completed_at;
