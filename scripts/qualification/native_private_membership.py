"""Pure inspection of the private native membership table for qualification.

The existing container owner executes QUERY once after Recall stops, on its
original paired network and clock. This module has no transport or file access.
Ordinary snapshots continue to exclude this private journal projection.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import uuid

TABLE = "source_erasure_membership"
RAW_CAP = 32 * 1024 * 1024
ENTRY_CAP = 100_000
HEX = re.compile(r"[a-f0-9]{64}\Z")
TABLE_DEFINITION = "DEFINE TABLE source_erasure_membership TYPE ANY SCHEMALESS PERMISSIONS NONE"
INDEX_DEFINITION = ("DEFINE INDEX source_erasure_membership_space_token ON "
                    "source_erasure_membership FIELDS space_id, token UNIQUE")
TABLE_SCHEMA = {"events": {}, "fields": {}, "indexes": {
    "source_erasure_membership_space_token": INDEX_DEFINITION}, "lives": {}, "tables": {}}
DATABASE_CATEGORIES = {"accesses", "analyzers", "apis", "configs", "functions", "models",
                       "params", "tables", "users"}

# Fixed read-only statements. Native type witnesses accompany the entire row;
# JSON's simplified record/UUID/string representations are not type authority.
# object::keys also exposes fields that a simplified JSON serializer may omit.
QUERY = """INFO FOR DB;
INFO FOR TABLE source_erasure_membership;
SELECT count() AS count FROM source_erasure_membership GROUP ALL;
SELECT VALUE {
    row: $this,
    keys: object::keys($this),
    id_is_record: type::is::record(id, 'source_erasure_membership'),
    id_is_string: type::is::string(record::id(id)),
    id_inner: record::id(id),
    space_is_string: type::is::string(space_id),
    token_is_string: type::is::string(token)
} FROM source_erasure_membership;
SELECT count() AS count FROM source_erasure_membership GROUP ALL;
"""


class Incomplete(ValueError):
    """Missing, malformed or unreviewed evidence cannot establish coverage."""


class Violation(ValueError):
    """Complete observed private state contradicts the payload-free contract."""


def need(value, reason):
    if not value:
        raise Incomplete(reason)


def invariant(value, reason):
    if not value:
        raise Violation(reason)


def encoded(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False,
                       separators=(",", ":")) + "\n").encode()


def decode(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            need(key not in result, "duplicate_private_json_key")
            result[key] = value
        return result

    def finite(value):
        number = float(value)
        need(math.isfinite(number), "nonfinite_private_json")
        return number

    return json.loads(raw, object_pairs_hook=pairs, parse_float=finite,
                      parse_constant=lambda _: need(False, "nonfinite_private_json"))


def membership_key(space, token):
    # Existing storage stable_key: UTF-8 byte lengths, UUIDv5 OID namespace.
    value = f"{len(space.encode('utf-8'))}:{space}{len(token.encode('utf-8'))}:{token}"
    return str(uuid.uuid5(uuid.NAMESPACE_OID, value))


def count(value):
    if value == []:
        return 0
    need(type(value) is list and len(value) == 1 and type(value[0]) is dict
         and set(value[0]) == {"count"} and type(value[0]["count"]) is int
         and 0 <= value[0]["count"] <= ENTRY_CAP, "private_count_shape_or_bound")
    return value[0]["count"]


def expected_memberships(tokens):
    need(type(tokens) is dict and len(tokens) <= ENTRY_CAP, "original_membership_mapping")
    expected = set()
    for space, values in tokens.items():
        need(type(space) is str and space and type(values) in (set, frozenset, list, tuple),
             "original_membership_space_tokens")
        need(len(values) <= ENTRY_CAP and len(set(values)) == len(values),
             "original_membership_duplicate_or_bound")
        for token in values:
            need(type(token) is str and HEX.fullmatch(token), "original_membership_token")
            need(len(expected) < ENTRY_CAP, "original_membership_total_bound")
            expected.add((space, token))
    return expected


def contains_canary(value, canary):
    pending = [value]
    visited = 0
    while pending:
        item = pending.pop()
        visited += 1
        need(visited <= RAW_CAP, "private_decoded_node_bound")
        if type(item) is str and canary in item:
            return True
        if type(item) is dict:
            pending.extend(item.keys())
            pending.extend(item.values())
        elif type(item) is list:
            pending.extend(item)
    return False


def _validate(raw, tokens, canary):
    need(type(raw) is bytes and 0 < len(raw) <= RAW_CAP, "private_inspection_raw_bound")
    need(type(canary) is str and canary, "original_private_canary_required")
    value = decode(raw)
    # A positive canary survives unrelated malformed schema/status elsewhere.
    invariant(not contains_canary(value, canary), "erased_canary_in_private_response")
    expected = expected_memberships(tokens)
    need(type(value) is list and len(value) == 5, "complete_five_private_results")
    for statement in value:
        need(type(statement) is dict and set(statement) == {"time", "status", "result"}
             and statement["status"] == "OK" and type(statement["time"]) is str
             and 0 < len(statement["time"]) <= 64, "private_statement_status_or_shape")

    # This fixed INFO query reads schema, never the journal or session bindings.
    # Root credentials are not DB users. Reject unexpected secret-bearing DB
    # schema rather than emitting its contents in the derived evidence/report.
    database, schema = value[0]["result"], value[1]["result"]
    need(type(database) is dict and set(database) == DATABASE_CATEGORIES
         and all(type(items) is dict for items in database.values()), "private_database_schema_shape")
    need(not any(database[name] for name in ("users", "accesses", "params")),
         "private_database_secret_schema_not_supported")
    need(database["tables"].get(TABLE) == TABLE_DEFINITION and schema == TABLE_SCHEMA,
         "unreviewed_private_table_schema")

    rows = value[3]["result"]
    need(type(rows) is list and len(rows) <= ENTRY_CAP, "private_complete_rows_bound")
    before, after = count(value[2]["result"]), count(value[4]["result"])
    need(before == len(rows) == after, "private_count_rows_changed_or_incomplete")
    observed, identities, per_space = set(), set(), {}
    witnesses = {"row", "keys", "id_is_record", "id_is_string", "id_inner",
                 "space_is_string", "token_is_string"}
    for item in rows:
        need(type(item) is dict and set(item) == witnesses and type(item["row"]) is dict,
             "private_native_row_witness_shape")
        row = item["row"]
        invariant(set(row) == {"id", "space_id", "token"}, "private_membership_payload_fields")
        invariant(type(item["keys"]) is list and len(item["keys"]) == 3
                  and set(item["keys"]) == set(row), "private_native_hidden_or_extra_field")
        invariant(all(item[name] is True for name in
                      ("id_is_record", "id_is_string", "space_is_string", "token_is_string")),
                  "private_membership_native_types")
        invariant(type(row["space_id"]) is str and row["space_id"]
                  and type(row["token"]) is str and HEX.fullmatch(row["token"]),
                  "private_membership_payload_shape")
        invariant(all(canary not in part for part in (row["space_id"], row["token"], str(row["id"]))),
                  "erased_canary_in_private_membership")
        key = membership_key(row["space_id"], row["token"])
        # Pinned Thing/Id Display backticks a String containing UUID hyphens.
        invariant(item["id_inner"] == key and row["id"] == TABLE + ":`" + key + "`",
                  "private_membership_native_identity")
        identity = (row["space_id"], row["token"])
        invariant(identity not in observed and key not in identities, "duplicate_private_membership")
        observed.add(identity)
        identities.add(key)
        per_space[row["space_id"]] = per_space.get(row["space_id"], 0) + 1
    invariant(observed == expected, "private_membership_original_journal_inventory")
    return {"format": "recall-native-private-membership-inspection/v1", "table": TABLE,
            "raw_sha256": hashlib.sha256(raw).hexdigest(), "raw_bytes": len(raw),
            "query_sha256": hashlib.sha256(QUERY.encode()).hexdigest(),
            "table_definition": TABLE_DEFINITION, "table_schema": schema,
            "count_before": before, "count_after": after, "row_count": len(rows),
            "space_counts": per_space, "rows": rows,
            "ordinary_snapshot_contains_private_rows": False, "product_acceptance": False}


def validate(raw, tokens, canary):
    """Audit complete raw SQL results against original nonsecret journal tokens.

    The caller separately proves stopped Recall, original paired-root authority,
    selected namespace/database, exact HTTP framing, original clock and budget,
    matching exported guard counts, and stopped-volume custody. This function
    never decides physical-erasure acceptance or substitutes count-only evidence.
    """
    try:
        return _validate(raw, tokens, canary)
    except (Incomplete, Violation):
        raise
    except (KeyError, TypeError, ValueError, OverflowError, RecursionError, UnicodeError) as error:
        raise Incomplete("malformed_private_evidence_" + type(error).__name__) from None
