-- Per-space processor approvals and storage-owned reindex progress.
DEFINE FIELD IF NOT EXISTS processing ON TABLE space_config TYPE option<object> FLEXIBLE;
-- Content mutation and activation write the same config row. A successful
-- activation therefore cannot race a content write without a write conflict.
DEFINE EVENT OVERWRITE source_index_generation ON TABLE source WHEN $event INSIDE ['CREATE', 'UPDATE', 'DELETE'] THEN {
    LET $space = IF $event = 'DELETE' THEN $before.space_id ELSE $after.space_id END;
    IF $event = 'UPDATE' AND $before.space_id != $after.space_id {
        UPDATE space_config SET processing.index_generation += 1, updated_at = time::now() WHERE space_id = $before.space_id AND deleted_at IS NONE AND processing IS NOT NONE;
    };
    UPDATE space_config SET processing.index_generation += 1, updated_at = time::now()
        WHERE space_id = $space AND deleted_at IS NONE AND processing IS NOT NONE;
};
DEFINE EVENT OVERWRITE doc_index_generation ON TABLE doc WHEN $event INSIDE ['CREATE', 'UPDATE', 'DELETE'] THEN {
    LET $space = IF $event = 'DELETE' THEN $before.space_id ELSE $after.space_id END;
    IF $event = 'UPDATE' AND $before.space_id != $after.space_id {
        UPDATE space_config SET processing.index_generation += 1, updated_at = time::now() WHERE space_id = $before.space_id AND deleted_at IS NONE AND processing IS NOT NONE;
    };
    UPDATE space_config SET processing.index_generation += 1, updated_at = time::now()
        WHERE space_id = $space AND deleted_at IS NONE AND processing IS NOT NONE;
};
DEFINE EVENT OVERWRITE revision_index_generation ON TABLE doc_revision WHEN $event INSIDE ['CREATE', 'UPDATE', 'DELETE'] THEN {
    LET $space = IF $event = 'DELETE' THEN $before.space_id ELSE $after.space_id END;
    IF $event = 'UPDATE' AND $before.space_id != $after.space_id {
        UPDATE space_config SET processing.index_generation += 1, updated_at = time::now() WHERE space_id = $before.space_id AND deleted_at IS NONE AND processing IS NOT NONE;
    };
    UPDATE space_config SET processing.index_generation += 1, updated_at = time::now()
        WHERE space_id = $space AND deleted_at IS NONE AND processing IS NOT NONE;
};
DEFINE EVENT OVERWRITE node_index_generation ON TABLE doc_node WHEN $event INSIDE ['CREATE', 'UPDATE', 'DELETE'] THEN {
    LET $space = IF $event = 'DELETE' THEN $before.space_id ELSE $after.space_id END;
    IF $event = 'UPDATE' AND $before.space_id != $after.space_id {
        UPDATE space_config SET processing.index_generation += 1, updated_at = time::now() WHERE space_id = $before.space_id AND deleted_at IS NONE AND processing IS NOT NONE;
    };
    UPDATE space_config SET processing.index_generation += 1, updated_at = time::now()
        WHERE space_id = $space AND deleted_at IS NONE AND processing IS NOT NONE;
};

DEFINE EVENT OVERWRITE entity_index_generation ON TABLE entity WHEN $event INSIDE ['CREATE', 'UPDATE', 'DELETE'] THEN {
    LET $space = IF $event = 'DELETE' THEN $before.space_id ELSE $after.space_id END;
    IF $event = 'UPDATE' AND $before.space_id != $after.space_id {
        UPDATE space_config SET processing.index_generation += 1, updated_at = time::now() WHERE space_id = $before.space_id AND deleted_at IS NONE AND processing IS NOT NONE;
    };
    UPDATE space_config SET processing.index_generation += 1, updated_at = time::now()
        WHERE space_id = $space AND deleted_at IS NONE AND processing IS NOT NONE;
};

-- Derived profile partitions survive staging without modifying canonical entities.
DEFINE TABLE IF NOT EXISTS entity_profile_embedding SCHEMALESS;
DEFINE INDEX IF NOT EXISTS entity_profile_partition ON TABLE entity_profile_embedding FIELDS space_id, model, entity_id UNIQUE;
