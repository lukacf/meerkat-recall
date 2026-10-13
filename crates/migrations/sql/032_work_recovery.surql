-- Queue attempt authority survives retry-counter reset and terminal archives.
DEFINE TABLE IF NOT EXISTS work_attempt SCHEMALESS;
DEFINE INDEX IF NOT EXISTS work_attempt_space_work ON work_attempt FIELDS space_id, work_item_id UNIQUE;
DEFINE INDEX IF NOT EXISTS work_attempt_recovery ON work_attempt FIELDS status, lease_expires_at;
-- Consumer acknowledgment is space local and advances only after durable effects.
DEFINE TABLE IF NOT EXISTS outbox_checkpoint SCHEMALESS;
DEFINE INDEX IF NOT EXISTS outbox_checkpoint_consumer ON outbox_checkpoint FIELDS space_id, consumer UNIQUE;
DEFINE INDEX IF NOT EXISTS work_item_expired_lease ON work_item FIELDS status, lease_expires_at;

-- Actual request receipts, including dispatched-without-measured-result.
DEFINE TABLE IF NOT EXISTS provider_call_receipt SCHEMALESS;
DEFINE INDEX IF NOT EXISTS provider_receipt_scope ON provider_call_receipt FIELDS space_id, scope_id;
