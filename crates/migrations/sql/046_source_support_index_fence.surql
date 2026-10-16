DEFINE FUNCTION OVERWRITE fn::recall_support_index_fence_valid($row: object) {
    RETURN object::len($row)=3
        AND $row.id=canonical_epoch:['recall_source_support_index',1]
        AND $row.space_id='__recall_source_support_index__'
        AND type::is::string($row.support_nonce)
        AND string::is::uuid($row.support_nonce);
};
-- Exact support lookups require validated payload/projection consistency. A
-- global nonce invalidates the bounded validation cache on every support change,
-- including malformed or cross-space projections. Ordinary vector/graph writes
-- do not change this nonce. It is derived validation metadata, not publication.
DEFINE EVENT OVERWRITE source_support_index_fence ON TABLE source_effect_support
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    UPSERT canonical_epoch:['recall_source_support_index',1] SET
        space_id='__recall_source_support_index__', support_nonce=<string>rand::uuid();
};
UPSERT canonical_epoch:['recall_source_support_index',1] SET
    space_id='__recall_source_support_index__', support_nonce=<string>rand::uuid();
