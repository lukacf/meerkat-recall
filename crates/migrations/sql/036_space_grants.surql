-- Current per-space subject grants; token ceilings do not replace this authority.
DEFINE TABLE IF NOT EXISTS space_grant SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS space_id ON space_grant TYPE string;
DEFINE FIELD IF NOT EXISTS subject ON space_grant TYPE string;
DEFINE FIELD IF NOT EXISTS payload ON space_grant TYPE string;
DEFINE INDEX IF NOT EXISTS space_grant_identity ON space_grant FIELDS space_id, subject UNIQUE;

-- A revoked grant fences every in-flight canonical authority observation.
DEFINE EVENT OVERWRITE grant_canonical_epoch ON TABLE space_grant
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" { $before.space_id } ELSE { $after.space_id };
    UPSERT type::thing('canonical_epoch', $space) SET space_id = $space, epoch = <string>rand::uuid();
};
