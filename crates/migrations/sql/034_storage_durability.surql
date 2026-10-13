-- The independently retained journal remains authoritative. This immutable
-- database identity detects an empty/substituted journal after data-volume restore.
DEFINE TABLE IF NOT EXISTS recall_durability SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS journal_id ON TABLE recall_durability TYPE string;
