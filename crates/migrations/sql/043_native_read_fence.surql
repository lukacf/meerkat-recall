-- Payload-free bounded physical-key writer coordination. The same database
-- function receives the actual typed identity from readers and write events.
-- At most 4096 buckets per allowed table (12288 total); no source/space/content
-- identity, complete hash, or per-record history is retained in these rows.
DEFINE TABLE IF NOT EXISTS native_read_fence SCHEMALESS;
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE native_read_fence;
REMOVE EVENT IF EXISTS erased_reference_fence ON TABLE native_read_fence;
-- Strict row shape is enforced on every ordinary write and checked before
-- restoring reference custody. The exemption never admits arbitrary payload.
DEFINE FUNCTION OVERWRITE fn::recall_physical_read_fence_valid($row: object) {
    RETURN object::len($row)=3
        AND type::is::record($row.id,'native_read_fence')
        AND type::is::string(meta::id($row.id))
        AND string::matches(meta::id($row.id),/^(doc|source_head|source_publication_manifest):[0-9a-f]{3}$/)
        AND type::is::int($row.version) AND $row.version=1
        AND type::is::string($row.nonce) AND string::len($row.nonce)=36
        AND string::is::uuid($row.nonce)
        AND string::matches($row.nonce,/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
};
DEFINE EVENT OVERWRITE physical_read_fence_shape ON TABLE native_read_fence
WHEN $event IN ['CREATE','UPDATE'] THEN {
    IF !fn::recall_physical_read_fence_valid($after) {
        THROW 'publication:invalid_native_read_fence';
    };
};
DEFINE FUNCTION OVERWRITE fn::recall_physical_read_fence($record: record) {
    LET $table = meta::tb($record);
    IF $table NOT IN ['doc','source_head','source_publication_manifest'] {
        THROW 'publication:unsupported_physical_read_fence';
    };
    LET $bucket = string::slice(crypto::sha256(type::string($record)), 0, 3);
    LET $id = type::thing('native_read_fence', string::concat($table, ':', $bucket));
    LET $existing = SELECT * FROM ONLY $id;
    IF $existing != NONE AND !fn::recall_physical_read_fence_valid($existing) {
        THROW 'publication:invalid_native_read_fence';
    };
    UPSERT $id SET version=1,nonce=<string>rand::uuid();
    RETURN true;
};

DEFINE EVENT OVERWRITE physical_read_fence ON TABLE doc
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event='DELETE' { fn::recall_physical_read_fence($before.id); } ELSE {
        fn::recall_physical_read_fence($after.id);
        IF $event='UPDATE' AND $before.id!=$after.id { fn::recall_physical_read_fence($before.id); };
    };
};
DEFINE EVENT OVERWRITE physical_read_fence ON TABLE source_head
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event='DELETE' { fn::recall_physical_read_fence($before.id); } ELSE {
        fn::recall_physical_read_fence($after.id);
        IF $event='UPDATE' AND $before.id!=$after.id { fn::recall_physical_read_fence($before.id); };
    };
};
DEFINE EVENT OVERWRITE physical_read_fence ON TABLE source_publication_manifest
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event='DELETE' { fn::recall_physical_read_fence($before.id); } ELSE {
        fn::recall_physical_read_fence($after.id);
        IF $event='UPDATE' AND $before.id!=$after.id { fn::recall_physical_read_fence($before.id); };
    };
};

-- Heads retain erased tombstone identities. They participate in original
-- journal reservation and actual shared writer revision, never live-reference
-- rejection. This exact special case is also used by dynamic guard refresh.
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE source_head;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_head
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event='DELETE' { fn::recall_reference_guard($before,false); } ELSE {
        fn::recall_reference_guard($after,false);
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};

-- Finalization reads these existing rows under exact original space/payload
-- guards. Their ordinary writers must touch the same actual reference revision;
-- the original 039 static list omitted them. Dynamic restore uses the same rule.
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE source_document_mapping;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_document_mapping
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event='DELETE' { fn::recall_reference_guard($before,false); } ELSE {
        fn::recall_reference_guard($after,true);
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE source_bundle_registration;
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_bundle_registration
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event='DELETE' { fn::recall_reference_guard($before,false); } ELSE {
        fn::recall_reference_guard($after,true);
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
