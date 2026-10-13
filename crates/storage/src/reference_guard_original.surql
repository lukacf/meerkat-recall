-- Test-only original reference guard from source 252cacadc4a793db93203b808e680db2c3d25a6c.
-- This frozen oracle is never installed by the runtime.
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
