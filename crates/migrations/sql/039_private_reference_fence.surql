-- The retained durability anchor is SCHEMAFULL since 034. Declare the private
-- key commitment before fresh initialization writes it; otherwise SurrealDB
-- silently discards the field and the original-key check correctly refuses.
-- This does not populate/replace keys or repair an established missing anchor.
DEFINE FIELD IF NOT EXISTS reference_key_fingerprint ON TABLE recall_durability TYPE string;

-- Requires the pinned Recall reference-token builtin version 1.
-- Private runtime key and journal identity are inherited query context values.
-- This function runs on the actual native postimage inside the writing transaction.
DEFINE TABLE IF NOT EXISTS source_erasure_guard SCHEMALESS;
DEFINE TABLE IF NOT EXISTS source_erasure_membership SCHEMALESS;
DEFINE INDEX IF NOT EXISTS source_erasure_guard_space ON source_erasure_guard FIELDS space_id UNIQUE;
DEFINE INDEX IF NOT EXISTS source_erasure_membership_space_token ON source_erasure_membership FIELDS space_id,token UNIQUE;
DEFINE FUNCTION OVERWRITE fn::recall_reference_guard($postimage: object, $check_references: bool) {
    LET $space = $postimage.space_id;
    IF $space = NONE OR $space = '' { THROW 'publication:private_reference_space_missing'; };
    IF $__recall_reference_key = NONE OR $__recall_journal_identity = NONE { THROW 'publication:private_erasure_key_unavailable'; };
    LET $fingerprints = recall::reference_tokens($__recall_journal_identity, '__recall_custody__', $__recall_reference_key, [], type::thing('recall_reference_key', $__recall_journal_identity));
    LET $fingerprint = $fingerprints[0];
    LET $guard_id = type::thing('source_erasure_guard', $space);
    LET $existing_guard = SELECT * FROM ONLY $guard_id;
    IF $existing_guard = NONE {
        CREATE $guard_id SET space_id=$space,journal_id=$__recall_journal_identity,key_fingerprint=$fingerprint,version=1,membership_count=0,revision=0;
    } ELSE {
        IF $existing_guard.journal_id != $__recall_journal_identity OR $existing_guard.key_fingerprint != $fingerprint OR $existing_guard.version != 1 { THROW 'publication:private_erasure_authority_mismatch'; };
    };
    LET $guard = SELECT * FROM ONLY $guard_id;
    IF $guard.erasure_reservation != NONE AND $guard.erasure_reservation != $__recall_erasure_reservation { THROW 'publication:erasure_space_reserved'; };
    IF !type::is::int($guard.revision) OR $guard.revision < 0 OR $guard.revision >= 9223372036854775807 { THROW 'publication:private_erasure_revision_exhausted'; };
    IF $check_references {
    LET $memberships = SELECT token FROM source_erasure_membership WHERE space_id=$space;
    IF array::len($memberships) != $guard.membership_count { THROW 'publication:private_erasure_membership_replay_incomplete'; };
    LET $references = recall::reference_tokens($__recall_journal_identity, $space, $__recall_reference_key, ["entity", "rel", "doc", "media_asset", "doc_chunk", "chunk_embedding", "entity_identity_key", "system_event", "event", "source", "audit_log", "id_map", "identity_candidate", "rel_evidence", "entity_merge", "space_config", "doc_revision", "doc_node", "assertion", "attribute", "work_item", "work_item_archive", "work_edge", "work_run", "work_run_archive", "work_artifact", "work_deadletter", "predicate_registry", "predicate_alias", "conflict_group", "truth_slot", "ingest_decision", "cost_ledger_daily", "freshness", "reprocess_job", "knowledge_view", "knowledge_view_revision", "knowledge_view_definition", "knowledge_view_dependency", "source_watch", "source_watch_check"], $postimage);
    LET $matches = SELECT id FROM source_erasure_membership WHERE space_id=$space AND token IN $references;
    IF array::len($matches)>0 { THROW 'publication:source_erased'; };
    -- Force a shared write conflict with membership publication, including queue
    -- and artifact writes which intentionally do not advance canonical_epoch.
    };
    UPDATE $guard_id SET revision=$guard.revision+1;
    RETURN true;
};

REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE entity;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE entity
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('entity'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE rel;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE rel
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('rel'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE doc;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE doc
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('doc'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE media_asset;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE media_asset
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('media_asset'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE doc_chunk;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE doc_chunk
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('doc_chunk'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE chunk_embedding;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE chunk_embedding
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('chunk_embedding'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE entity_identity_key;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE entity_identity_key
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('entity_identity_key'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE system_event;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE system_event
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('system_event'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE event;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE event
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('event'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE source;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('source'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE audit_log;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE audit_log
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('audit_log'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE id_map;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE id_map
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('id_map'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE identity_candidate;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE identity_candidate
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('identity_candidate'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE rel_evidence;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE rel_evidence
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('rel_evidence'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE entity_merge;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE entity_merge
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('entity_merge'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE space_config;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE space_config
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('space_config'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE doc_revision;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE doc_revision
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('doc_revision'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE doc_node;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE doc_node
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('doc_node'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE assertion;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE assertion
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('assertion'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE attribute;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE attribute
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('attribute'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE work_item;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE work_item
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('work_item'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE work_item_archive;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE work_item_archive
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('work_item_archive'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE work_edge;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE work_edge
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('work_edge'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE work_run;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE work_run
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('work_run'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE work_run_archive;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE work_run_archive
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('work_run_archive'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE work_artifact;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE work_artifact
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('work_artifact'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE work_deadletter;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE work_deadletter
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('work_deadletter'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE predicate_registry;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE predicate_registry
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('predicate_registry'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE predicate_alias;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE predicate_alias
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('predicate_alias'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE conflict_group;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE conflict_group
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('conflict_group'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE truth_slot;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE truth_slot
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('truth_slot'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE ingest_decision;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE ingest_decision
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('ingest_decision'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE cost_ledger_daily;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE cost_ledger_daily
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('cost_ledger_daily'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE freshness;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE freshness
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('freshness'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE reprocess_job;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE reprocess_job
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('reprocess_job'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE knowledge_view;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE knowledge_view
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('knowledge_view'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE knowledge_view_revision;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE knowledge_view_revision
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('knowledge_view_revision'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE knowledge_view_definition;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE knowledge_view_definition
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('knowledge_view_definition'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE knowledge_view_dependency;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE knowledge_view_dependency
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('knowledge_view_dependency'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE source_watch;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_watch
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('source_watch'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE source_watch_check;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_watch_check
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('source_watch_check'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE entity_profile_embedding;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE entity_profile_embedding
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('entity_profile_embedding'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE source_revision;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_revision
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('source_revision'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE source_effect_support;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_effect_support
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('source_effect_support'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE source_commit_receipt;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_commit_receipt
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('source_commit_receipt'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE source_decision_receipt;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_decision_receipt
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('source_decision_receipt'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE source_revision_mapping;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_revision_mapping
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('source_revision_mapping'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE source_publication_manifest;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_publication_manifest
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('source_publication_manifest'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE source_publication_manifest_history;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_publication_manifest_history
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('source_publication_manifest_history'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE source_publication_completion;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_publication_completion
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('source_publication_completion'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE work_attempt;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE work_attempt
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('work_attempt'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE provider_call_receipt;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE provider_call_receipt
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event = 'DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, !('provider_call_receipt'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
UPSERT publication_schema:reference_guard SET version=1;
