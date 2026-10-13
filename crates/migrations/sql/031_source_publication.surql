DEFINE TABLE IF NOT EXISTS source_authorization SCHEMALESS;
DEFINE INDEX IF NOT EXISTS source_authorization_space_source ON source_authorization FIELDS space_id,source_id UNIQUE;
-- Source desired/published authority and canonical predicate fencing.
-- Control payloads are explicit serialized DTOs, not flattened driver records.
DEFINE TABLE OVERWRITE source_identity SCHEMALESS;
DEFINE TABLE IF NOT EXISTS source_decision_receipt SCHEMALESS;
DEFINE TABLE IF NOT EXISTS source_decision_sequence SCHEMALESS;
DEFINE INDEX IF NOT EXISTS source_decision_space_sequence ON source_decision_receipt FIELDS space_id,sequence;
DEFINE TABLE OVERWRITE source_head SCHEMALESS;
DEFINE TABLE OVERWRITE source_revision SCHEMALESS;
DEFINE TABLE OVERWRITE source_operation SCHEMALESS;
DEFINE TABLE OVERWRITE source_effect_support SCHEMALESS;
DEFINE TABLE OVERWRITE source_commit_receipt SCHEMALESS;
DEFINE TABLE OVERWRITE canonical_epoch SCHEMALESS;
DEFINE TABLE OVERWRITE publication_schema SCHEMALESS;
DEFINE INDEX OVERWRITE source_head_space_source ON source_head FIELDS space_id, source_id UNIQUE;
DEFINE INDEX OVERWRITE source_revision_space_source ON source_revision FIELDS space_id, source_id;
DEFINE INDEX OVERWRITE source_operation_space_source ON source_operation FIELDS space_id, source_id;
DEFINE INDEX OVERWRITE source_support_space_source ON source_effect_support FIELDS space_id, source_id;
DEFINE INDEX OVERWRITE source_support_space_record ON source_effect_support FIELDS space_id, record_id;
DEFINE INDEX OVERWRITE source_commit_space_source ON source_commit_receipt FIELDS space_id, source_id;

-- Every canonical write, including trusted repositories and raw background
-- updates, conflicts the same per-space row. Capture before planning, compare
-- and write inside publication. This fences absent matches and range changes.
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE entity
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE rel
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE doc
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE media_asset
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE chunk_embedding
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE entity_identity_key
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE event
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE source
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE id_map
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE identity_candidate
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE rel_evidence
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE entity_merge
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE space_config
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE doc_revision
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE doc_node
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE assertion
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE attribute
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
-- Work artifacts are staging state; exact effect/read guards and sealed manifests
-- fence them without invalidating the semantic epoch carried through staging.
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE predicate_registry
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE predicate_alias
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE conflict_group
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE truth_slot
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE ingest_decision
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE freshness
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE reprocess_job
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE knowledge_view
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE knowledge_view_revision
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE knowledge_view_definition
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE knowledge_view_dependency
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE source_watch
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE source_watch_check
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};


-- Permanent payload-free reference fences survive erasure and snapshot replay.
DEFINE TABLE OVERWRITE source_tombstone_ref SCHEMALESS;
DEFINE TABLE OVERWRITE source_erasure_files SCHEMALESS;
DEFINE INDEX OVERWRITE erased_reference_space ON source_tombstone_ref FIELDS space_id;
DEFINE INDEX OVERWRITE erased_reference_source ON source_tombstone_ref FIELDS space_id, source_id;
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE source_identity
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE source_head
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE source_revision
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE source_effect_support
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE entity
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE rel
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE doc
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE media_asset
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE chunk_embedding
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE entity_identity_key
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE system_event
WHEN $event IN ["CREATE", "UPDATE"] AND $after.event_type != 'source.erased' THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE event
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE audit_log
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE id_map
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE identity_candidate
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE rel_evidence
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE entity_merge
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE space_config
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE doc_revision
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE doc_node
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE assertion
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE attribute
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE work_item
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE work_item_archive
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE work_edge
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE work_run
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE work_run_archive
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE work_artifact
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE work_deadletter
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE predicate_registry
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE predicate_alias
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE conflict_group
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE truth_slot
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE ingest_decision
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE freshness
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE reprocess_job
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE knowledge_view
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE knowledge_view_revision
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE knowledge_view_definition
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE knowledge_view_dependency
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_watch
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_watch_check
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_effect_support
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};

DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE source_authorization
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE entity_profile_embedding
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE entity_profile_embedding
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
DEFINE INDEX IF NOT EXISTS system_event_space_seq ON TABLE system_event FIELDS space_id,seq UNIQUE;

DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE source_decision_receipt
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id=$space,epoch=<string>rand::uuid(); };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_decision_receipt
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};

-- Written last: publication refuses a database lacking the complete fence.
UPSERT publication_schema:canonical CONTENT { version: 1 };
