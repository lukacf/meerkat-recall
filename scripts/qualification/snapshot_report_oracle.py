"""Independent, pure physical-export predicates for the bounded Metadata fixture.

No imports of product selectors, database access, filesystem reads, subprocesses,
network calls, keys, or implicit execution. A pass is an exported-inventory
component only; live storage/media/WAL/backup-retention require separate evidence.
"""
from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import math
import re
import uuid
from dataclasses import dataclass

REPORT_CAP = 32 * 1024 * 1024 + 1
ROWS_CAP = 200_000
NODES_CAP = 2_000_000
LEAF_CAP = 14_065_536
EXPANDED_CAP = 64 * 1024 * 1024
HEX = re.compile(r"[0-9a-f]{64}")
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
DOMAIN = re.compile(r"([a-z_][a-z0-9_]*):([0-9a-f-]{36})")
CONTROL_TABLES = {"source_head", "source_authorization", "source_identity", "source_operation",
                  "source_tombstone_ref", "source_erasure_guard"}
BASE_FIELDS = {"id", "space_id", "created_at", "updated_at", "created_by", "deleted_at",
               "security", "provenance", "subjects", "subjects_state"}
# D independently checked all 2374 original bodies against the pinned Runtime16
# source, plus E's separate original reference-template review. These are SHA256
# of sorted complete definition-body SHA lists, not a union of allowed rows.
# See native-schema-profile-runtime16.json and PRISTINE_SCHEMA_REVIEW.md.
REVIEWED_SCHEMA_PROFILES = {
    "423b64d32a22485ebb3bf185ad858a9914393785c6722e612340ff03d6839a8e":
        ("fresh-migrations-001-through-043", 1186),
    "3d874af9b747a2b5e467301b3058e0b18ec413a0df6026eb852aa70bce113ec1":
        ("standard-restore-refreshed", 1188),
}
REVIEWED_SCHEMA_TABLES = frozenset("""
assertion attribute audit_log canonical_epoch chunk_embedding conflict_group
cost_ledger_daily doc doc_chunk doc_node doc_revision entity entity_identity_key
entity_merge entity_profile_embedding event freshness id_map identity_candidate
ingest_decision knowledge_view knowledge_view_definition knowledge_view_dependency
knowledge_view_revision media_asset migration_history native_read_fence
outbox_checkpoint predicate_alias predicate_registry provider_call_receipt
publication_schema recall_durability rel rel_evidence reprocess_job source
source_authorization source_bundle_registration source_commit_receipt
source_contact_intent source_decision_receipt source_decision_sequence
source_document_mapping source_effect_support source_erasure_files
source_erasure_guard source_head source_identity source_operation
source_publication_completion source_publication_manifest
source_publication_manifest_history source_revision source_revision_mapping
source_tombstone_ref source_watch source_watch_check space_config space_grant
system_event truth_slot watch_delegation work_artifact work_attempt work_deadletter
work_edge work_item work_item_archive work_producer_origin work_run work_run_archive
""".split())


class Incomplete(ValueError):
    """The supplied evidence cannot establish a complete component verdict."""


class Violation(ValueError):
    """Complete evidence contradicts a required physical invariant."""


def need(condition, code):
    if not condition:
        raise Incomplete(code)


def invariant(condition, code):
    if not condition:
        raise Violation(code)


def fields(value, required, optional=()):
    need(type(value) is dict and set(required) <= set(value)
         and set(value) <= set(required) | set(optional), "unknown_or_missing_fields")


def encoded(value, *, sorted_keys=False):
    return json.dumps(value, ensure_ascii=False, sort_keys=sorted_keys,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            need(key not in result, "duplicate_json_key")
            result[key] = value
        return result
    def finite(value):
        parsed = float(value)
        need(math.isfinite(parsed), "nonfinite_json")
        return parsed
    return json.loads(raw, object_pairs_hook=pairs, parse_float=finite,
                      parse_constant=lambda _: (_ for _ in ()).throw(Incomplete("nonfinite_json")))


def timestamp(value):
    need(type(value) is str and len(value) <= 64, "timestamp_shape")
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    need(parsed.tzinfo is not None, "timestamp_timezone")
    return parsed


@dataclass(frozen=True)
class Reference:
    table: str
    identifier: str


@dataclass(frozen=True)
class Atom:
    kind: str
    value: str = ""


def source_reference(value):
    need(type(value) is str, "source_identity_shape")
    match = DOMAIN.fullmatch(value)
    need(match is not None and match[1] == "source" and UUID.fullmatch(match[2]),
         "source_identity_shape")
    return Reference("source", match[2])


class TaggedDecoder:
    def __init__(self, blobs=None):
        self.nodes = 0
        self.compact = blobs is not None
        self.blobs, self.used, self.expanded = {}, set(), 0
        if blobs is not None:
            need(type(blobs) is dict and len(blobs) <= NODES_CAP, "blob_dictionary_shape")
            total = 0
            for key, entry in blobs.items():
                need(type(key) is str and HEX.fullmatch(key), "blob_key_shape")
                fields(entry, {"bytes", "base64"})
                size, spelling = entry["bytes"], entry["base64"]
                need(type(size) is int and 1024 <= size <= LEAF_CAP
                     and type(spelling) is str and re.fullmatch(r"[A-Za-z0-9+/]*", spelling)
                     and len(spelling) == (size * 8 + 5) // 6, "blob_shape_or_leaf_bound")
                total += size
                need(total <= EXPANDED_CAP, "unique_blob_byte_limit")
                raw = base64.b64decode(spelling + "=" * (-len(spelling) % 4), validate=True)
                need(len(raw) == size and digest(raw) == key
                     and base64.b64encode(raw).decode().rstrip("=") == spelling,
                     "blob_length_hash_or_base64")
                self.blobs[key] = raw

    def leaf(self, size, account):
        need(size <= LEAF_CAP, "leaf_byte_limit")
        if self.compact and account:
            self.expanded += size
            need(self.expanded <= EXPANDED_CAP, "expanded_leaf_byte_limit")

    def identity(self, value, depth=0, account=False):
        fields(value, {"table", "id"})
        need(type(value["table"]) is str and 0 < len(value["table"].encode()) <= REPORT_CAP,
             "table_identity_shape")
        tagged, logical = self.value(value["id"], depth + 1, refs=False, account=account)
        canonical = {"table": value["table"], "id": tagged}
        reference = (Reference(value["table"], logical) if type(logical) is str
                     else Atom("composite_identity", encoded(canonical).decode()))
        return canonical, reference

    def value(self, value, depth=0, *, refs=False, account=False):
        self.nodes += 1
        need(self.nodes <= NODES_CAP and depth <= 128, "value_budget")
        need(type(value) is dict and type(value.get("kind")) is str, "tagged_value_shape")
        kind = value["kind"]
        if kind in ("none", "null"):
            fields(value, {"kind"})
            return {"kind": kind}, Atom("none") if kind == "none" else None
        fields(value, {"kind", "value"})
        item = value["value"]
        if kind in ("string_ref", "bytes_ref"):
            need(self.compact and refs and type(item) is str and item in self.blobs,
                 "dangling_or_misplaced_blob_reference")
            self.used.add(item)
            raw = self.blobs[item]
            kind = "string" if kind == "string_ref" else "bytes"
            item = raw.decode("utf-8") if kind == "string" else raw.hex()
        if kind == "bool":
            need(type(item) is bool, "bool_shape")
            logical = item
        elif kind == "int":
            need(type(item) is int and -(2**63) <= item < 2**63, "integer_shape")
            logical = item
        elif kind in ("string", "uuid", "datetime", "uint", "decimal", "float_bits", "bytes"):
            need(type(item) is str and len(item.encode()) <= REPORT_CAP, "scalar_shape")
            if kind == "uuid":
                need(UUID.fullmatch(item), "uuid_shape")
            elif kind == "datetime":
                timestamp(item)
            elif kind == "uint":
                need(re.fullmatch(r"[0-9]+", item) and 2**63 <= int(item) < 2**64, "unsigned_shape")
            elif kind == "float_bits":
                need(re.fullmatch(r"[0-9a-f]{16}", item), "float_bits_shape")
            elif kind == "bytes":
                need(len(item) % 2 == 0 and re.fullmatch(r"[0-9a-f]*", item), "bytes_shape")
            elif kind == "decimal":
                need(re.fullmatch(r"-?[0-9]+(?:\.[0-9]+)?", item), "decimal_shape")
            logical = item if kind in ("string", "uuid", "datetime") else Atom(kind, item)
            if kind == "string":
                self.leaf(len(item.encode()), account)
            elif kind == "bytes":
                self.leaf(len(item) // 2, account)
        elif kind == "duration":
            fields(item, {"seconds", "nanoseconds"})
            need(type(item["seconds"]) is int and 0 <= item["seconds"] < 2**64
                 and type(item["nanoseconds"]) is int and 0 <= item["nanoseconds"] < 10**9,
                 "duration_shape")
            item = {"seconds": item["seconds"], "nanoseconds": item["nanoseconds"]}
            logical = Atom(kind, encoded(item).decode())
        elif kind == "thing":
            item, logical = self.identity(item, depth + 1, account=account)
        elif kind == "array":
            need(type(item) is list, "array_shape")
            pairs = [self.value(child, depth + 1, refs=refs, account=account) for child in item]
            item, logical = [p[0] for p in pairs], [p[1] for p in pairs]
        elif kind == "map":
            need(type(item) is dict and all(type(key) is str for key in item), "map_shape")
            pairs = {key: self.value(item[key], depth + 1, refs=refs, account=account) for key in sorted(item)}
            item, logical = {k: p[0] for k, p in pairs.items()}, {k: p[1] for k, p in pairs.items()}
        else:
            raise Incomplete("unsupported_tagged_value")
        return {"kind": kind, "value": item}, logical


def reference(value):
    if isinstance(value, Reference):
        return value
    if type(value) is str:
        match = DOMAIN.fullmatch(value)
        if match is not None and UUID.fullmatch(match[2]):
            return Reference(match[1], match[2])
    # Embedded RecordId storage objects are recognized structurally, not by a
    # substring in prose. Native number/string/composite identities stay tagged
    # for inventory equality even when they cannot be domain UUID references.
    if type(value) is dict and set(value) == {"tb", "id"}:
        if type(value["tb"]) is str and type(value["id"]) is str:
            return Reference(value["tb"], value["id"])
    return None


def physical_record_identity(value, table):
    if isinstance(value, Reference):
        return value
    if type(value) is str and value.startswith(table + ":"):
        return Reference(table, value[len(table) + 1:])
    if type(value) is dict and set(value) == {"tb", "id"}:
        return reference(value)
    raise Incomplete("unsupported_physical_record_identity")


def walk(value, *, depth=0):
    need(depth <= 128, "nested_payload_depth")
    yield value
    if type(value) is dict:
        for key, child in value.items():
            yield key
            yield from walk(child, depth=depth + 1)
    elif type(value) is list:
        for child in value:
            yield from walk(child, depth=depth + 1)
    elif type(value) is str and value[:1] in ("{", "["):
        try:
            decoded = strict_json(value)
        except (json.JSONDecodeError, UnicodeError):
            return  # Ordinary source prose need not itself be JSON.
        yield from walk(decoded, depth=depth + 1)


def reviewed_schema_body(statement):
    """Strip only pinned exporter section headers, preserving the exact SQL body."""
    remaining = statement.lstrip(" \t\r\n")
    headers = []
    while remaining.startswith("--"):
        match = re.match(r"-- ------------------------------\n-- ([^\n]+)\n"
                         r"-- ------------------------------(?:\n|$)", remaining)
        need(match is not None and len(headers) < 2, "unreviewed_native_schema_header")
        title = match[1]
        if title not in ("FUNCTIONS", "ANALYZERS"):
            table = re.fullmatch(r"TABLE(?: DATA)?: ([a-z_][a-z0-9_]*)", title)
            need(table is not None and table[1] in REVIEWED_SCHEMA_TABLES,
                 "unreviewed_native_schema_header")
        headers.append(title)
        remaining = remaining[match.end():].lstrip(" \t\r\n")
    body = remaining.rstrip(" \t\r\n")
    need(body.startswith("DEFINE ") and body.endswith(";"), "unreviewed_native_schema_statement")
    body = body[:-1]
    if headers:
        need(all(title.startswith("TABLE DATA: ") for title in headers[:-1]),
             "unreviewed_native_schema_header")
        last = headers[-1]
        if last in ("FUNCTIONS", "ANALYZERS"):
            need(body.startswith("DEFINE " + last[:-1] + " "), "unreviewed_native_schema_header")
        else:
            need(last.startswith("TABLE: ") and body.startswith("DEFINE TABLE " + last[7:] + " "),
                 "unreviewed_native_schema_header")
    return body


def reviewed_schema_profile(schema, backend, canary):
    need(type(schema) is list, "schema_inventory_shape")
    if backend == "elephant-db":
        need(not schema, "unexpected_embedded_schema_statement")
        return "embedded-no-native-schema"
    hashes = set()
    for statement in schema:
        fields(statement, {"sha256", "statement"})
        need(type(statement["statement"]) is str
             and digest(statement["statement"].encode()) == statement["sha256"], "schema_digest_mismatch")
        invariant(canary not in statement["statement"], "erased_canary_in_schema")
        body_hash = digest(reviewed_schema_body(statement["statement"]).encode())
        need(body_hash not in hashes, "duplicate_native_schema_definition")
        hashes.add(body_hash)
    profile = REVIEWED_SCHEMA_PROFILES.get(digest(encoded(sorted(hashes))))
    need(profile is not None and len(hashes) == profile[1], "unreviewed_native_schema_statement")
    return profile[0]


def report_inventory(report, expected_manifest, backend, fixture):
    compact = report.get("format") == "recall-snapshot-inspection-v2"
    fields(report, {"format", "manifest", "table_counts", "row_count", "statement_count", "rows", "schema_statements"}
           | ({"blobs"} if compact else set()))
    need(report["format"] in ("recall-snapshot-inspection-v1", "recall-snapshot-inspection-v2")
         and report["manifest"] == expected_manifest and expected_manifest["backend"] == backend,
         "report_manifest_identity")
    need(len(encoded(report)) <= REPORT_CAP, "report_byte_limit")
    rows, counts = report["rows"], report["table_counts"]
    need(type(rows) is list and type(report["row_count"]) is int
         and len(rows) == report["row_count"] <= ROWS_CAP, "complete_row_count")
    need(type(counts) is dict and len(counts) <= 256
         and all(type(k) is str and type(v) is int and v >= 0 for k, v in counts.items()),
         "table_count_shape")
    decoder = TaggedDecoder(report["blobs"] if compact else None)
    inventory, observed, references_count = {}, {}, 0
    for row in rows:
        fields(row, {"identity", "space_id", "sha256", "references", "record"})
        identity, ref = decoder.identity(row["identity"])
        key = encoded(identity)
        need(key not in inventory and identity["table"] in counts, "duplicate_or_unknown_identity")
        record, logical = decoder.value(row["record"], refs=compact, account=True)
        need(record["kind"] == "map", "non_object_record")
        need(physical_record_identity(logical.get("id"), identity["table"]) == ref,
             "record_outer_physical_identity_mismatch")
        need(type(row["sha256"]) is str and HEX.fullmatch(row["sha256"])
             and digest(encoded(record)) == row["sha256"], "record_digest_mismatch")
        space = logical.get("space_id")
        need(space is None or space == Atom("none") or type(space) is str, "stored_space_shape")
        need(row["space_id"] == (space if type(space) is str else None), "projected_space_mismatch")
        need(type(row["references"]) is list and len(row["references"]) <= 100_000,
             "reference_inventory_shape")
        references_count += len(row["references"])
        need(references_count <= 100_000, "aggregate_reference_limit")
        for item in row["references"]:
            fields(item, {"path", "identity"})
            need(type(item["path"]) is str, "reference_path_shape")
            decoder.identity(item["identity"])
        # Reference discovery below uses complete records, never the inspector's
        # reference list or digest as a substitute for physical payload bytes.
        refs, canaries = set(), {"A": False, "B": False}
        for value in walk(logical):
            if (found := reference(value)) is not None:
                refs.add(found)
            if type(value) is str:
                for side in canaries:
                    canaries[side] |= fixture[side]["canary"] in value
        table = identity["table"]
        controls = CONTROL_TABLES | {"system_event", "native_read_fence", "canonical_epoch"}
        retained = None
        if table in controls:
            need(len(encoded(record)) <= 64 * 1024, "control_row_size")
            retained = logical
        positives = {}
        for side in ("A", "B"):
            target = fixture[side]
            if space == target["space_id"] and table in ("source", "source_revision"):
                positives[side] = original_source_positive(table, ref, logical, target)
        # Only bounded controls, identity and independently recomputed digest
        # survive this iteration. Never retain the expanded complete inventory.
        inventory[key] = {"identity": identity, "reference": ref, "table": identity["table"],
                          "space": row["space_id"], "record": retained, "sha256": digest(encoded(record)),
                          "references": refs, "canaries": canaries, "positives": positives}
        observed[identity["table"]] = observed.get(identity["table"], 0) + 1
    need(all(observed.get(table, 0) == count for table, count in counts.items())
         and sum(counts.values()) == len(rows), "incomplete_table_inventory")
    need(decoder.used == set(decoder.blobs), "orphan_blob_dictionary_entry")
    schema = report["schema_statements"]
    need(type(schema) is list and type(report["statement_count"]) is int
         and len(schema) <= report["statement_count"] <= 100_000, "schema_inventory_shape")
    profile = reviewed_schema_profile(schema, backend, fixture["A"]["canary"])
    return inventory, profile


def journal_intents(entries):
    need(type(entries) is list and len(entries) <= 16, "journal_inventory_limit")
    result, total = {}, 0
    for entry in entries:
        fields(entry, {"filename", "sha256", "bytes", "body_base64", "intent"})
        intent = entry["intent"]
        fields(intent, {"intent_id", "space_id", "source_id", "recorded_at"})
        need(type(intent["intent_id"]) is str and UUID.fullmatch(intent["intent_id"]), "intent_identity")
        name = intent["intent_id"] + ".json"
        need(entry["filename"] == name and name not in result, "intent_filename")
        raw = base64.b64decode(entry["body_base64"], validate=True)
        need(type(entry["bytes"]) is int and 0 < len(raw) == entry["bytes"] <= 4096
             and digest(raw) == entry["sha256"] and strict_json(raw) == intent, "intent_original_bytes")
        source_reference(intent["source_id"])
        timestamp(intent["recorded_at"])
        total += len(raw)
        need(total <= 128 * 1024, "journal_total_limit")
        result[name] = entry
    return result


def fixture_contract(fixture):
    fields(fixture, {"format", "backend", "journal", "A", "B", "erasure", "snapshots"})
    need(fixture["format"] == "recall-physical-fixture/v1"
         and fixture["backend"] in ("elephant-db", "surreal-db"), "fixture_format")
    for side in (fixture["A"], fixture["B"]):
        fields(side, {"space_id", "source_id", "external_key", "revision_id", "canary",
                      "accepted_receipt", "completed_baseline"})
        source_reference(side["source_id"])
        need(all(type(side[k]) is str and side[k] for k in
                 ("space_id", "external_key", "revision_id", "canary")), "fixture_identity")
        need(len(side["canary"].encode()) <= 1024 and side["canary"].startswith("urn:recall:qualification:"),
             "metadata_uri_canary")
        fields(side["completed_baseline"], {"head", "operation", "source"})
        source = side["completed_baseline"]["source"]
        need(source["id"] == side["source_id"] and source["space_id"] == side["space_id"]
             and source["external_id"] == side["external_key"]
             and source["current_ingest_mode"] == "metadata" and source["lifecycle_status"] == "metadata_ready",
             "original_metadata_positive")
    a, b = fixture["A"], fixture["B"]
    need(a["space_id"] != b["space_id"] and a["source_id"] != b["source_id"]
         and a["canary"] not in b["canary"] and b["canary"] not in a["canary"], "distinct_fixture_scopes")
    journal = fixture["journal"]
    fields(journal, {"journal_id", "reference_key_fingerprint", "before_intents", "after_intents",
                     "original_erasure_intent", "erase_started_at", "erase_completed_observed_at"})
    need(UUID.fullmatch(journal["journal_id"]) and HEX.fullmatch(journal["reference_key_fingerprint"]),
         "journal_identity")
    before, after = journal_intents(journal["before_intents"]), journal_intents(journal["after_intents"])
    invariant(all(after.get(name) == entry for name, entry in before.items()), "original_journal_intent_changed")
    added = set(after) - set(before)
    need(len(added) == 1, "one_original_new_erasure_intent_required")
    intent = after[next(iter(added))]["intent"]
    need(intent == journal["original_erasure_intent"] and intent["space_id"] == a["space_id"]
         and intent["source_id"] == a["source_id"]
         and timestamp(journal["erase_started_at"]) <= timestamp(intent["recorded_at"])
         <= timestamp(journal["erase_completed_observed_at"]), "original_intent_completion_window")
    fields(fixture["snapshots"], {"pre", "post", "replay"})
    for snapshot in fixture["snapshots"].values():
        fields(snapshot, {"capture", "sha256", "bytes", "manifest"})
        need(type(snapshot["capture"]) is str and HEX.fullmatch(snapshot["sha256"])
             and type(snapshot["bytes"]) is int and 0 < snapshot["bytes"] <= REPORT_CAP,
             "raw_snapshot_capture_custody")
        manifest = snapshot["manifest"]
        fields(manifest, {"version", "format", "snapshot_id", "journal_id", "backend", "name",
                          "created_at", "expires_at", "bytes", "sha256"})
        need(manifest["version"] == 1 and manifest["format"] == "recall-snapshot-v1"
             and UUID.fullmatch(manifest["snapshot_id"]) and HEX.fullmatch(manifest["sha256"])
             and type(manifest["bytes"]) is int and 0 < manifest["bytes"] <= EXPANDED_CAP,
             "snapshot_manifest_shape")
        need(manifest["journal_id"] == journal["journal_id"] and manifest["backend"] == fixture["backend"]
             and timestamp(manifest["created_at"]) < timestamp(manifest["expires_at"]), "original_snapshot_journal")
    need(len({v["manifest"]["snapshot_id"] for v in fixture["snapshots"].values()}) == 3,
         "three_distinct_actual_snapshots_required")
    fields(fixture["erasure"], {"accepted", "completed"})
    completed = fixture["erasure"]["completed"]
    need(completed["state"] == "complete" and completed["lifecycle"] == "erased"
         and completed["cleanup_state"] == "complete" and completed["internal_source_id"] == a["source_id"]
         and completed["operation_id"] == fixture["erasure"]["accepted"]["operation_id"], "actual_erasure_completion")
    return intent


def absent(value):
    return value is None or value == Atom("none")


def normalized(value):
    if isinstance(value, Reference):
        return f"{value.table}:{value.identifier}"
    if value == Atom("none"):
        return None
    if type(value) is dict:
        return {key: normalized(child) for key, child in value.items()}
    if type(value) is list:
        return [normalized(child) for child in value]
    return value


def control_payload(row):
    fields(row, {"id", "space_id", "source_id", "payload", "state"},
           {"canonical_read_nonce", "read_nonce"})
    for name in ("canonical_read_nonce", "read_nonce"):
        if name in row:
            need(type(row[name]) is str and UUID.fullmatch(row[name]), "control_nonce_shape")
    need(type(row["payload"]) is str, "control_payload_shape")
    payload = strict_json(row["payload"])
    need(normalized(row["state"]) == payload, "control_state_payload_divergence")
    return payload


def original_source_positive(table, identity, row, side):
    target = source_reference(side["source_id"])
    if table == "source":
        source = normalized(row)
        return (identity == target and source.get("id") == side["source_id"]
                and source.get("space_id") == side["space_id"]
                and source.get("uri") == side["canary"]
                and source.get("external_id") == side["external_key"]
                and source.get("lifecycle_status") == "metadata_ready")
    stored = control_payload(row)
    fields(stored, {"revision", "operation_id", "input_fingerprint"})
    revision = stored["revision"]
    fields(revision, {"space_id", "source_id", "revision_id", "content_hash", "accepted_at", "source"})
    source = revision["source"]
    return (revision["space_id"] == side["space_id"] and revision["source_id"] == side["source_id"]
            and revision["revision_id"] == side["revision_id"]
            and stored["operation_id"] == side["accepted_receipt"]["operation_id"]
            and source["id"] == side["source_id"] and source["space_id"] == side["space_id"]
            and source.get("uri") == side["canary"] and source["external_id"] == side["external_key"])


def envelope(value, fixture):
    fields(value, {"space_id", "source_id", "security", "subjects", "subjects_state"})
    source = fixture["A"]["completed_baseline"]["source"]
    expected = {key: source[key] for key in ("space_id", "security", "subjects", "subjects_state")}
    expected["source_id"] = fixture["A"]["source_id"]
    invariant(normalized(value) == expected, "retained_authorization_changed")


def private_operation(value, fixture, before):
    fields(value, {"receipt", "request_fingerprint", "input_fingerprint", "authorization",
                   "contact_digest", "producer_origin", "predecessor_publication"})
    envelope(value["authorization"], fixture)
    need(absent(value["contact_digest"]), "fixture_requires_metadata_operation")
    for name in ("request_fingerprint", "input_fingerprint"):
        need(type(value[name]) is str and re.fullmatch(r"(?:sha256:)?[0-9a-f]{64}", value[name]),
             "operation_fingerprint_shape")
    predecessor = value["predecessor_publication"]
    if not absent(predecessor):
        fields(predecessor, {"source_id", "generation", "revision_id", "operation_id"})
        original = fixture["A"]["accepted_receipt"]
        need(predecessor["source_id"] == fixture["A"]["source_id"]
             and predecessor["revision_id"] == fixture["A"]["revision_id"]
             and predecessor["operation_id"] == original["operation_id"]
             and predecessor["generation"] == original["source_generation"], "original_predecessor_identity")
    receipt = value["receipt"]
    fields(receipt, {"space_id", "operation_id", "source_id", "source_generation", "desired_revision",
                     "published_revision", "state", "accepted_at", "completed_at", "error_code"})
    need(receipt["space_id"] == fixture["A"]["space_id"]
         and receipt["source_id"] == fixture["A"]["source_id"], "operation_source_identity")
    invariant(all(absent(receipt[name]) for name in ("desired_revision", "published_revision", "error_code")),
              "operation_payload_not_sanitized")
    timestamp(receipt["accepted_at"])
    timestamp(receipt["completed_at"])
    current = fixture["erasure"]["completed"]
    if receipt["operation_id"] == current["operation_id"]:
        need(value["producer_origin"] == {"kind": "unknown_original"}, "erasure_origin_shape")
        invariant(receipt["state"] == "erased" and receipt["source_generation"] == current["generation"],
                  "erasure_receipt_not_terminal")
    else:
        need(before is not None, "unknown_retained_operation")
        need(value["producer_origin"] == {"kind": "direct"}, "original_direct_intake_required")
        accepted = fixture["A"]["accepted_receipt"]
        need(receipt["operation_id"] == accepted["operation_id"]
             and receipt["source_generation"] == accepted["source_generation"], "original_accepted_operation_binding")
        invariant(receipt["state"] == "superseded", "old_operation_not_superseded")
        old = control_payload(before["record"])
        for name in set(value) - {"receipt"}:
            invariant(value[name] == old[name], "original_operation_control_changed")
        for name in ("operation_id", "source_generation", "accepted_at", "space_id", "source_id"):
            invariant(receipt[name] == old["receipt"][name], "original_operation_identity_changed")


def payload_free_fence(row, fixture):
    table, value = row["table"], row["record"]
    if table == "native_read_fence":
        fields(value, {"id", "version", "nonce"})
        ref = row["reference"]
        need(isinstance(ref, Reference) and ref.table == table
             and re.fullmatch(r"(?:doc|source_head|source_publication_manifest):[0-9a-f]{3}", ref.identifier)
             and type(value["version"]) is int and value["version"] == 1 and type(value["nonce"]) is str
             and UUID.fullmatch(value["nonce"]), "native_bucket_shape")
    elif table == "canonical_epoch":
        fields(value, {"id", "space_id", "epoch"}, {"standing_watch_read_nonce"})
        need(type(value["space_id"]) is str and type(value["epoch"]) is str
             and UUID.fullmatch(value["epoch"]), "canonical_epoch_shape")
        if "standing_watch_read_nonce" in value:
            need(UUID.fullmatch(value["standing_watch_read_nonce"]), "canonical_nonce_shape")
    else:
        fields(value, {"id", "space_id", "journal_id", "key_fingerprint", "version",
                       "membership_count", "revision"}, {"erasure_reservation"})
        need(value["journal_id"] == fixture["journal"]["journal_id"]
             and value["key_fingerprint"] == fixture["journal"]["reference_key_fingerprint"]
             and type(value["version"]) is int and value["version"] == 1 and absent(value.get("erasure_reservation"))
             and all(type(value[name]) is int and value[name] >= 0
                     for name in ("membership_count", "revision")), "journal_guard_shape")


def erased_event(row, fixture, original_refs):
    value = row["record"]
    fields(value, BASE_FIELDS | {"seq", "event_type", "payload"})
    need(value["event_type"] == "source.erased" and value["created_by"] == "source-lifecycle"
         and type(value["seq"]) is int and value["seq"] >= 0 and absent(value["deleted_at"]),
         "retained_event_shape")
    operation = fixture["erasure"]["completed"]["operation_id"]
    need(row["reference"] == Reference("system_event", str(uuid.uuid5(uuid.NAMESPACE_OID,
         operation + ":source.erased"))), "original_erasure_event_identity")
    envelope({"space_id": value["space_id"], "source_id": fixture["A"]["source_id"],
              **{key: value[key] for key in ("security", "subjects", "subjects_state")}}, fixture)
    provenance = value["provenance"]
    fields(provenance, {"sources", "extractor", "run_id", "work_run_id", "prompt_id", "config_id", "chain"})
    need(provenance["sources"] == [] and provenance["chain"] == []
         and all(absent(provenance[k]) for k in set(provenance) - {"sources", "chain"}),
         "erasure_event_provenance_payload")
    payload = value["payload"]
    if type(payload) is str:
        payload = strict_json(payload)
    fields(payload, {"space_id", "source_id", "operation_id", "records"})
    need(payload["space_id"] == fixture["A"]["space_id"]
         and reference(payload["source_id"]) == source_reference(fixture["A"]["source_id"])
         and payload["operation_id"] == operation and type(payload["records"]) is list,
         "erasure_event_operation")
    need(payload["records"], "erasure_event_records_empty")
    for item in payload["records"]:
        fields(item, {"id", "table", "op"})
        ref = reference(item["id"])
        need(ref in original_refs and item["table"] == ref.table
             and item["op"] == ("delete" if ref == source_reference(fixture["A"]["source_id"])
                                else "update"), "erasure_event_unknown_record")


def retained_control(row, fixture, intent, original, original_refs):
    table, value = row["table"], row["record"]
    need(value is not None and row["space"] == fixture["A"]["space_id"], "retained_control_scope")
    if table == "system_event":
        erased_event(row, fixture, original_refs)
        return
    if table == "source_tombstone_ref":
        fields(value, {"id", "space_id", "source_id", "token", "intent_id"})
        need(reference(value["source_id"]) == source_reference(fixture["A"]["source_id"])
             and value["intent_id"] == intent["intent_id"] and type(value["token"]) is str,
             "tombstone_original_intent")
        valid_tokens = {ref.identifier for ref in original_refs} | {
            f"{ref.table}:{ref.identifier}" for ref in original_refs}
        need(value["token"] in valid_tokens, "tombstone_unknown_reference")
        return
    payload = control_payload(value)
    need(reference(value["source_id"]) == source_reference(fixture["A"]["source_id"]),
         "retained_source_identity")
    if table == "source_head":
        fields(payload, {"space_id", "source_id", "generation", "desired_revision", "published_revision",
                         "operation_id", "lifecycle", "updated_at"})
        actual = fixture["erasure"]["completed"]
        invariant(payload["lifecycle"] == "erased" and absent(payload["desired_revision"])
                  and absent(payload["published_revision"]), "head_not_payload_free_erased")
        need(payload["space_id"] == fixture["A"]["space_id"]
             and payload["source_id"] == fixture["A"]["source_id"]
             and payload["generation"] == actual["generation"]
             and payload["operation_id"] == actual["operation_id"], "head_original_operation")
        timestamp(payload["updated_at"])
    elif table == "source_authorization":
        envelope(payload, fixture)
    elif table == "source_identity":
        fields(payload, {"source_id"})
        need(payload["source_id"] == fixture["A"]["source_id"], "retained_identity")
    elif table == "source_operation":
        private_operation(payload, fixture, original)
    else:
        raise Incomplete("unknown_retained_source_table")


def evaluate_exports(pre_report: dict, post_report: dict, replay_report: dict, fixture: dict) -> dict:
    """Return a component verdict only; raw capture custody belongs to the caller.

    Complete physical inventories are mandatory. Unknown structure is UNMET,
    an established invariant violation is FAIL. This function never asserts
    product acceptance, media/WAL erasure, registered backup expiry or live DB
    correctness. Its caller must bind these parsed objects to captured raw SHA.
    """
    result = {"format": "recall-export-oracle/v1", "scope": "exported_inventory",
              "product_acceptance": False, "raw_capture_custody": "caller_required",
              "verdict": "unmet", "checks": []}
    try:
        intent = fixture_contract(fixture)
        reports = {"pre": pre_report, "post": post_report, "replay": replay_report}
        decoded = {name: report_inventory(report, fixture["snapshots"][name]["manifest"],
                   fixture["backend"], fixture) for name, report in reports.items()}
        inventories = {name: value[0] for name, value in decoded.items()}
        schema_profiles = {name: value[1] for name, value in decoded.items()}
        invariant(schema_profiles["post"] == schema_profiles["pre"], "post_schema_inventory_changed")
        expected_replay = ("standard-restore-refreshed" if fixture["backend"] == "surreal-db"
                           else "embedded-no-native-schema")
        invariant(schema_profiles["replay"] == expected_replay, "replay_schema_profile_not_refreshed")
        pre = inventories["pre"]
        for side in ("A", "B"):
            for table in ("source", "source_revision"):
                need(any(row["table"] == table and row["positives"].get(side) for row in pre.values()),
                     "original_populated_source_and_revision_required")
        result["checks"].append("original_populated_sources_and_immutable_revisions")
        a_ref = source_reference(fixture["A"]["source_id"])
        closure_refs, exclusive = {a_ref}, set()
        while True:
            added = {key for key, row in pre.items() if key not in exclusive
                     and row["table"] not in CONTROL_TABLES
                     and (row["reference"] in closure_refs or row["canaries"]["A"]
                          or row["references"] & closure_refs)}
            if not added:
                break
            exclusive.update(added)
            closure_refs.update(pre[key]["reference"] for key in added
                                if isinstance(pre[key]["reference"], Reference))
        original_refs = {row["reference"] for row in pre.values() if isinstance(row["reference"], Reference)}
        original_b = {key: row["sha256"] for key, row in pre.items()
                      if row["space"] == fixture["B"]["space_id"]}
        need(original_b, "populated_independent_space_required")
        for name in ("post", "replay"):
            inventory = inventories[name]
            invariant(not (exclusive & inventory.keys()), name + "_source_exclusive_physical_rows_retained")
            current_b = {key: row["sha256"] for key, row in inventory.items()
                         if row["space"] == fixture["B"]["space_id"]}
            invariant(current_b == original_b, name + "_independent_space_full_inventory_changed")
            found_controls = set()
            for key, row in inventory.items():
                invariant(not row["canaries"]["A"], name + "_erased_canary_retained")
                if row["table"] in ("native_read_fence", "canonical_epoch", "source_erasure_guard"):
                    payload_free_fence(row, fixture)
                    continue
                if row["table"] == "source_erasure_membership":
                    raise Incomplete("private_membership_material_in_export")
                if (a_ref in row["references"] or row["references"] & closure_refs
                        or (row["space"] == fixture["A"]["space_id"]
                            and row["table"] in CONTROL_TABLES)):
                    retained_control(row, fixture, intent, pre.get(key), original_refs)
                    found_controls.add(row["table"])
            need({"source_head", "source_authorization", "source_identity", "source_operation",
                  "source_tombstone_ref"} <= found_controls, "required_retained_controls_missing")
            result["checks"].append(name + "_physical_absence_strict_controls_and_exact_independent_space")
        result["checks"].append("one_original_journal_intent_and_three_distinct_snapshot_generations")
        result["verdict"] = "pass"
    except Violation as error:
        result.update(verdict="fail", reason=str(error))
    except Incomplete as error:
        result.update(verdict="unmet", reason=str(error))
    except (KeyError, TypeError, ValueError, OverflowError, RecursionError, UnicodeError) as error:
        # Never echo input payloads or exception messages containing source data.
        result.update(verdict="unmet", reason="malformed_evidence_" + type(error).__name__)
    return result


def evaluate_export_bytes(pre: bytes, post: bytes, replay: bytes, fixture: dict) -> dict:
    """Preferred collector API: bind every parsed report to original raw bytes."""
    try:
        parsed = []
        for name, raw in (("pre", pre), ("post", post), ("replay", replay)):
            need(type(raw) is bytes and 0 < len(raw) <= REPORT_CAP, "raw_report_byte_limit")
            capture = fixture["snapshots"][name]
            need(capture["bytes"] == len(raw) and capture["sha256"] == digest(raw),
                 "raw_report_capture_mismatch")
            parsed.append(strict_json(raw))
        result = evaluate_exports(*parsed, fixture)
        result["raw_capture_custody"] = "verified_sha256_length_and_strict_json"
        return result
    except (Incomplete, KeyError, TypeError, ValueError, OverflowError, RecursionError, UnicodeError) as error:
        return {"format": "recall-export-oracle/v1", "scope": "exported_inventory",
                "product_acceptance": False, "raw_capture_custody": "unverified",
                "verdict": "unmet", "checks": [],
                "reason": str(error) if isinstance(error, Incomplete) else "malformed_raw_capture"}
