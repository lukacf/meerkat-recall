"""Synthetic closed-world decoder/predicate controls; never live acceptance.

No database, socket, subprocess or provider. The native schema literals are the
independently reviewed pinned-source catalog, never a runtime candidate export.
Source authored independently of the Rust inspector. Parent schedules execution.
"""
import base64
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("snapshot_report_oracle", Path(__file__).with_name("snapshot_report_oracle.py"))
ORACLE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = ORACLE
SPEC.loader.exec_module(ORACLE)

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "tests/recall_preview/native-schema-profile-runtime16.json"
SCHEMA_RAW = SCHEMA_PATH.read_bytes()
assert hashlib.sha256(SCHEMA_RAW).hexdigest() == "9ea4c0c850697993810bfb774b172cd35a7d79334a70b02b70ea7231c7cf8618"
SCHEMA = json.loads(SCHEMA_RAW)


def native_schema(restored=False):
    profile = "standard-restore-refreshed" if restored else "fresh-migrations-001-through-043"
    result = []
    for identity in SCHEMA["profiles"][profile]["definitions"]:
        statement = SCHEMA["definitions"][identity] + ";"
        result.append({"sha256": hashlib.sha256(statement.encode()).hexdigest(), "statement": statement})
    return result

NOW = "2026-10-07T02:00:00Z"
LATER = "2026-10-07T02:01:00Z"
IDS = [f"00000000-0000-4000-8000-{number:012d}" for number in range(1, 12)]


def raw(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def sha(value):
    return hashlib.sha256(value).hexdigest()


def tagged(value):
    if value is None:
        return {"kind": "null"}
    if type(value) is bool:
        return {"kind": "bool", "value": value}
    if type(value) is int:
        return {"kind": "int", "value": value}
    if type(value) is str:
        return {"kind": "string", "value": value}
    if type(value) is list:
        return {"kind": "array", "value": [tagged(item) for item in value]}
    return {"kind": "map", "value": {key: tagged(value[key]) for key in sorted(value)}}


def row(table, key, value, native=False):
    identity = {"table": table, "id": {"kind": "string", "value": key}}
    value = copy.deepcopy(value)
    value["id"] = table + ":" + key
    record = tagged(value)
    if native:
        record["value"]["id"] = {"kind": "thing", "value": copy.deepcopy(identity)}
    return {"identity": identity, "space_id": value.get("space_id"), "sha256": sha(raw(record)),
            "references": [], "record": record}


def control(table, key, source, value, native):
    return row(table, key, {"space_id": source["space_id"], "source_id": source["id"],
                          "payload": raw(value).decode(), "state": value}, native)


def refresh(report):
    report["row_count"] = len(report["rows"])
    counts = {}
    for item in report["rows"]:
        table = item["identity"]["table"]
        counts[table] = counts.get(table, 0) + 1
        item["sha256"] = sha(raw(item["record"]))
    report["table_counts"] = counts


def fixture(native=False):
    backend = "surreal-db" if native else "elephant-db"
    sides, rows = {}, []
    for index, label in enumerate(("A", "B")):
        sid = "source:" + IDS[index]
        space = "physical-" + label
        operation = "original-" + label
        source = {"id": sid, "space_id": space, "external_id": "shared-external-key",
                  "uri": "urn:recall:qualification:" + label, "current_ingest_mode": "metadata",
                  "lifecycle_status": "metadata_ready", "security": {"policy_id": "p-" + label},
                  "subjects": [], "subjects_state": "unknown"}
        sides[label] = {"space_id": space, "source_id": sid, "external_key": source["external_id"],
                        "revision_id": "r-" + label, "canary": source["uri"],
                        "accepted_receipt": {"operation_id": operation, "source_generation": "g-" + label},
                        "completed_baseline": {"head": {}, "operation": {}, "source": source}}
        rows.append(row("source", IDS[index], source, native))
        stored = {"revision": {"space_id": space, "source_id": sid, "revision_id": "r-" + label,
                               "content_hash": "1" * 64, "accepted_at": NOW, "source": source},
                  "operation_id": operation, "input_fingerprint": "2" * 64}
        rows.append(control("source_revision", "revision-" + label, source, stored, native))
    a = sides["A"]["completed_baseline"]["source"]
    authorization = {key: a[key] for key in ("space_id", "security", "subjects", "subjects_state")}
    authorization["source_id"] = a["id"]
    head = {"space_id": a["space_id"], "source_id": a["id"], "generation": "g-A",
            "desired_revision": "r-A", "published_revision": "r-A", "operation_id": "original-A",
            "lifecycle": "active", "updated_at": NOW}
    op = {"receipt": {"space_id": a["space_id"], "source_id": a["id"], "source_generation": "g-A",
                      "operation_id": "original-A", "desired_revision": "r-A", "published_revision": "r-A",
                      "state": "completed", "accepted_at": NOW, "completed_at": NOW, "error_code": None},
          "request_fingerprint": "3" * 64, "input_fingerprint": "4" * 64,
          "authorization": authorization, "contact_digest": None, "producer_origin": {"kind": "direct"},
          "predecessor_publication": None}
    rows += [control("source_head", "head-A", a, head, native),
             control("source_authorization", "auth-A", a, authorization, native),
             control("source_identity", "identity-A", a, {"source_id": a["id"]}, native),
             control("source_operation", "operation-A", a, op, native)]
    intent = {"intent_id": IDS[2], "space_id": a["space_id"], "source_id": a["id"], "recorded_at": NOW}
    body = raw(intent)
    evidence = {"format": "recall-physical-fixture/v1", "backend": backend, **sides,
                "journal": {"journal_id": IDS[3], "reference_key_fingerprint": "5" * 64,
                            "before_intents": [], "after_intents": [{"filename": IDS[2] + ".json",
                            "sha256": sha(body), "bytes": len(body), "body_base64": base64.b64encode(body).decode(),
                            "intent": intent}], "original_erasure_intent": intent,
                            "erase_started_at": NOW, "erase_completed_observed_at": LATER},
                "erasure": {"accepted": {"operation_id": "erase-A"},
                            "completed": {"operation_id": "erase-A", "generation": "erased-g-A",
                            "state": "complete", "lifecycle": "erased", "cleanup_state": "complete",
                            "internal_source_id": a["id"]}}, "snapshots": {}}
    post_rows = copy.deepcopy(rows[2:4])
    head.update(generation="erased-g-A", desired_revision=None, published_revision=None,
                operation_id="erase-A", lifecycle="erased", updated_at=LATER)
    op["receipt"].update(desired_revision=None, published_revision=None, state="superseded")
    post_rows += [control("source_head", "head-A", a, head, native),
                  control("source_authorization", "auth-A", a, authorization, native),
                  control("source_identity", "identity-A", a, {"source_id": a["id"]}, native),
                  control("source_operation", "operation-A", a, op, native)]
    new = copy.deepcopy(op)
    new["receipt"].update(operation_id="erase-A", source_generation="erased-g-A", state="erased")
    new["producer_origin"] = {"kind": "unknown_original"}
    new["predecessor_publication"] = {"source_id": a["id"], "generation": "g-A",
                                      "revision_id": "r-A", "operation_id": "original-A"}
    post_rows += [control("source_operation", "erase-operation", a, new, native),
                  row("source_tombstone_ref", "tombstone-A", {"space_id": a["space_id"],
                      "source_id": a["id"], "token": IDS[0], "intent_id": IDS[2]}, native)]
    reports = []
    for index, name in enumerate(("pre", "post", "replay")):
        manifest = {"version": 1, "format": "recall-snapshot-v1", "snapshot_id": IDS[index + 4],
                    "journal_id": IDS[3], "backend": backend, "name": name, "created_at": NOW,
                    "expires_at": "2026-10-08T02:00:00Z", "bytes": 100, "sha256": "6" * 64}
        report = {"format": "recall-snapshot-inspection-v1", "manifest": manifest,
                  "table_counts": {}, "row_count": 0, "statement_count": 0,
                  "rows": copy.deepcopy(rows if name == "pre" else post_rows), "schema_statements": []}
        if native:
            report["schema_statements"] = native_schema(restored=name == "replay")
            report["statement_count"] = len(report["schema_statements"])
        refresh(report)
        evidence["snapshots"][name] = {"capture": name + ".json", "sha256": sha(raw(report)),
                                       "bytes": len(raw(report)), "manifest": manifest}
        reports.append(report)
    return reports, evidence


class InventoryPredicates(unittest.TestCase):
    def test_both_typed_backend_fixtures_are_component_only(self):
        for native in (False, True):
            reports, evidence = fixture(native)
            result = ORACLE.evaluate_export_bytes(*(raw(v) for v in reports), evidence)
            self.assertEqual(result["verdict"], "pass", result)
            self.assertFalse(result["product_acceptance"])
            self.assertEqual(result["scope"], "exported_inventory")

    def test_soft_deleted_original_physical_source_fails(self):
        reports, evidence = fixture()
        survivor = copy.deepcopy(reports[0]["rows"][0])
        survivor["record"]["value"]["deleted_at"] = tagged(LATER)
        survivor["record"]["value"] = dict(sorted(survivor["record"]["value"].items()))
        reports[1]["rows"].append(survivor)
        refresh(reports[1])
        self.assertEqual(ORACLE.evaluate_exports(*reports, evidence)["verdict"], "fail")

    def test_canary_in_unknown_unowned_extension_fails(self):
        reports, evidence = fixture()
        reports[2]["rows"].append(row("opaque_extension", "opaque", {"body": evidence["A"]["canary"]}))
        refresh(reports[2])
        self.assertEqual(ORACLE.evaluate_exports(*reports, evidence)["verdict"], "fail")

    def test_independent_space_payload_timestamp_or_row_loss_fails(self):
        for mutation in ("payload", "timestamp", "delete"):
            reports, evidence = fixture()
            if mutation == "delete":
                del reports[1]["rows"][0]
            else:
                record = reports[1]["rows"][0]["record"]["value"]
                record[mutation] = tagged("changed")
                reports[1]["rows"][0]["record"]["value"] = dict(sorted(record.items()))
            refresh(reports[1])
            self.assertEqual(ORACLE.evaluate_exports(*reports, evidence)["verdict"], "fail")

    def test_unknown_retained_control_field_is_unmet(self):
        reports, evidence = fixture()
        record = reports[1]["rows"][2]["record"]["value"]
        record["hidden"] = tagged("unknown retained content")
        reports[1]["rows"][2]["record"]["value"] = dict(sorted(record.items()))
        refresh(reports[1])
        self.assertEqual(ORACLE.evaluate_exports(*reports, evidence)["verdict"], "unmet")

    def test_unknown_native_bucket_payload_is_unmet(self):
        reports, evidence = fixture(True)
        reports[1]["rows"].append(row("native_read_fence", "doc:abc",
            {"version": 1, "nonce": IDS[0], "hidden": "payload"}, True))
        refresh(reports[1])
        self.assertEqual(ORACLE.evaluate_exports(*reports, evidence)["verdict"], "unmet")

    def test_original_intent_cannot_be_replaced_by_new_intent(self):
        reports, evidence = fixture()
        evidence["journal"]["original_erasure_intent"] = {**evidence["journal"]["original_erasure_intent"],
                                                         "intent_id": IDS[9]}
        self.assertEqual(ORACLE.evaluate_exports(*reports, evidence)["verdict"], "unmet")

    def test_bool_version_and_mismatched_physical_identity_refuse(self):
        for corruption in ("bool_version", "identity"):
            reports, evidence = fixture(True)
            fence = row("native_read_fence", "doc:abc", {"version": 1, "nonce": IDS[0]}, True)
            if corruption == "bool_version":
                fence["record"]["value"]["version"] = tagged(True)
            else:
                fence["record"]["value"]["id"]["value"]["id"]["value"] = "doc:fff"
            reports[1]["rows"].append(fence)
            refresh(reports[1])
            self.assertEqual(ORACLE.evaluate_exports(*reports, evidence)["verdict"], "unmet")

    def test_unknown_identical_schema_cannot_be_its_own_authority(self):
        reports, evidence = fixture(True)
        statement = "DEFINE FIELD hidden ON doc DEFAULT 'undeclared content';"
        for report in reports:
            report["statement_count"] = 1
            report["schema_statements"] = [{"sha256": sha(statement.encode()), "statement": statement}]
        result = ORACLE.evaluate_exports(*reports, evidence)
        self.assertEqual(result["verdict"], "unmet")
        self.assertEqual(result["reason"], "unreviewed_native_schema_statement")

    def test_complete_native_profile_refuses_omission_and_duplicate(self):
        for duplicate in (False, True):
            reports, evidence = fixture(True)
            schema = reports[0]["schema_statements"]
            if duplicate:
                schema.append(copy.deepcopy(schema[0]))
            else:
                schema.pop()
            reports[0]["statement_count"] = len(schema)
            self.assertEqual(ORACLE.evaluate_exports(*reports, evidence)["verdict"], "unmet")

    def test_reviewed_definition_union_does_not_allow_mixed_profile(self):
        reports, evidence = fixture(True)
        fresh = {row["statement"] for row in native_schema()}
        different = next(row for row in native_schema(True) if row["statement"] not in fresh)
        reports[0]["schema_statements"][0] = different
        self.assertEqual(ORACLE.evaluate_exports(*reports, evidence)["verdict"], "unmet")

    def test_restore_requires_complete_standard_refresh_profile(self):
        reports, evidence = fixture(True)
        reports[2]["schema_statements"] = native_schema()
        reports[2]["statement_count"] = len(reports[2]["schema_statements"])
        result = ORACLE.evaluate_exports(*reports, evidence)
        self.assertEqual((result["verdict"], result["reason"]),
                         ("fail", "replay_schema_profile_not_refreshed"))

    def test_post_erasure_does_not_adopt_a_different_reviewed_profile(self):
        reports, evidence = fixture(True)
        reports[1]["schema_statements"] = native_schema(True)
        reports[1]["statement_count"] = len(reports[1]["schema_statements"])
        result = ORACLE.evaluate_exports(*reports, evidence)
        self.assertEqual((result["verdict"], result["reason"]),
                         ("fail", "post_schema_inventory_changed"))

    def test_known_export_headers_may_move_when_previous_table_contains_rows(self):
        reports, evidence = fixture(True)
        row = next(row for row in reports[0]["schema_statements"]
                   if row["statement"].startswith("DEFINE TABLE source "))
        separator = "-- ------------------------------\n"
        row["statement"] = (separator + "-- TABLE DATA: rel\n" + separator + "\n"
                            + separator + "-- TABLE: source\n" + separator + "\n" + row["statement"])
        row["sha256"] = sha(row["statement"].encode())
        self.assertEqual(ORACLE.evaluate_exports(*reports, evidence)["verdict"], "pass")

    def test_unknown_export_header_and_erased_comment_payload_are_rejected(self):
        for erased in (False, True):
            reports, evidence = fixture(True)
            row = reports[0]["schema_statements"][0]
            text = evidence["A"]["canary"] if erased else "unreviewed header payload"
            row["statement"] = "-- " + text + "\n" + row["statement"]
            row["sha256"] = sha(row["statement"].encode())
            result = ORACLE.evaluate_exports(*reports, evidence)
            self.assertEqual(result["verdict"], "fail" if erased else "unmet")

    def test_complete_profile_mutation_cannot_be_authorized_by_consistency(self):
        reports, evidence = fixture(True)
        for report in reports:
            row = next(row for row in report["schema_statements"]
                       if row["statement"].startswith("DEFINE TABLE source "))
            row["statement"] = row["statement"].replace("PERMISSIONS NONE", "PERMISSIONS FULL")
            row["sha256"] = sha(row["statement"].encode())
        self.assertEqual(ORACLE.evaluate_exports(*reports, evidence)["verdict"], "unmet")

    def test_original_accepted_generation_is_not_replaced_by_consistent_private_rows(self):
        reports, evidence = fixture()
        evidence["A"]["accepted_receipt"]["source_generation"] = "unrelated-generation"
        self.assertEqual(ORACLE.evaluate_exports(*reports, evidence)["verdict"], "unmet")

    def test_incomplete_empty_or_digest_only_report_is_unmet(self):
        for shape in ("empty", "missing_record", "wrong_count", "wrong_hash"):
            reports, evidence = fixture()
            if shape == "empty":
                reports[0]["rows"] = []
                refresh(reports[0])
            elif shape == "missing_record":
                del reports[0]["rows"][0]["record"]
            elif shape == "wrong_count":
                reports[0]["row_count"] += 1
            else:
                reports[0]["rows"][0]["sha256"] = "0" * 64
            self.assertEqual(ORACLE.evaluate_exports(*reports, evidence)["verdict"], "unmet")

    def test_prose_uuid_and_other_table_identity_are_not_source_references(self):
        reports, evidence = fixture()
        extra = row("opaque_extension", IDS[0], {"space_id": evidence["B"]["space_id"],
            "prose": "ordinary prose mentioning " + evidence["A"]["source_id"] + " suffix",
            "other": "entity:" + IDS[0]})
        for report in reports:
            report["rows"].append(copy.deepcopy(extra))
            refresh(report)
        self.assertEqual(ORACLE.evaluate_exports(*reports, evidence)["verdict"], "pass")

    def test_raw_hash_length_and_duplicate_keys_refuse(self):
        reports, evidence = fixture()
        captures = [raw(v) for v in reports]
        captures[0] += b" "
        self.assertEqual(ORACLE.evaluate_export_bytes(*captures, evidence)["verdict"], "unmet")
        for value in ('{"same":1,"same":2}', '{"x":NaN}', '{"x":1e999}'):
            with self.assertRaises(ORACLE.Incomplete):
                ORACLE.strict_json(value)


class CompactDecoder(unittest.TestCase):
    def blob(self, value):
        key = sha(value)
        return key, {key: {"bytes": len(value), "base64": base64.b64encode(value).decode().rstrip("=")}}

    def test_shared_raw_blob_preserves_string_and_bytes_types(self):
        key, blobs = self.blob(b"x" * 1024)
        decoder = ORACLE.TaggedDecoder(blobs)
        string, logical = decoder.value({"kind": "string_ref", "value": key}, refs=True, account=True)
        binary, _ = decoder.value({"kind": "bytes_ref", "value": key}, refs=True, account=True)
        self.assertEqual(string, tagged("x" * 1024))
        self.assertEqual(binary, {"kind": "bytes", "value": (b"x" * 1024).hex()})
        self.assertEqual(logical, "x" * 1024)
        self.assertEqual(decoder.expanded, 2048)

    def test_compact_inventory_verifies_original_expanded_row_hash(self):
        reports, evidence = fixture(True)
        text = "independent-original-content-" * 64
        key, blobs = self.blob(text.encode())
        for report in reports:
            item = next(item for item in report["rows"] if item["identity"]["table"] == "source"
                        and item["space_id"] == evidence["B"]["space_id"])
            item["record"]["value"]["title"] = tagged(text)
            item["record"]["value"] = dict(sorted(item["record"]["value"].items()))
            refresh(report)  # Hash ORIGINAL tagged row, before compaction.
            item["record"]["value"]["title"] = {"kind": "string_ref", "value": key}
            report.update(format="recall-snapshot-inspection-v2", blobs=copy.deepcopy(blobs))
        result = ORACLE.evaluate_exports(*reports, evidence)
        self.assertEqual(result["verdict"], "pass", result)
        reports[1]["rows"][0]["sha256"] = sha(raw(reports[1]["rows"][0]["record"]))
        result = ORACLE.evaluate_exports(*reports, evidence)
        self.assertEqual(result["verdict"], "unmet")
        self.assertEqual(result["reason"], "record_digest_mismatch")

    def test_noncanonical_base64_hash_length_and_unknown_fields_refuse(self):
        key, original = self.blob(b"x" * 1024)
        for change in ({"base64": original[key]["base64"] + "=="}, {"bytes": 1025},
                       {"extra": "hidden"}, {"base64": "A" * len(original[key]["base64"])}):
            blobs = copy.deepcopy(original)
            blobs[key].update(change)
            with self.assertRaises(ORACLE.Incomplete):
                ORACLE.TaggedDecoder(blobs)

    def test_dangling_misplaced_and_invalid_utf8_string_refs_refuse(self):
        key, blobs = self.blob(b"\xff" * 1024)
        decoder = ORACLE.TaggedDecoder(blobs)
        with self.assertRaises(UnicodeError):
            decoder.value({"kind": "string_ref", "value": key}, refs=True)
        for value, allowed in ((key, False), ("0" * 64, True)):
            with self.assertRaises(ORACLE.Incomplete):
                decoder.value({"kind": "bytes_ref", "value": value}, refs=allowed)
        with self.assertRaises(ORACLE.Incomplete):
            decoder.identity({"table": "doc", "id": {"kind": "bytes_ref", "value": key}})

    def test_every_reference_occurrence_counts_toward_expansion_bound(self):
        key, blobs = self.blob(b"x" * 1024)
        decoder = ORACLE.TaggedDecoder(blobs)
        with patch.object(ORACLE, "EXPANDED_CAP", 2048):
            for _ in range(2):
                decoder.value({"kind": "string_ref", "value": key}, refs=True, account=True)
            with self.assertRaises(ORACLE.Incomplete):
                decoder.value({"kind": "string_ref", "value": key}, refs=True, account=True)

    def test_orphan_dictionary_refuses_complete_report(self):
        reports, evidence = fixture()
        _, blobs = self.blob(b"unused" * 200)
        for report in reports:
            report.update(format="recall-snapshot-inspection-v2", blobs=blobs)
        result = ORACLE.evaluate_exports(*reports, evidence)
        self.assertEqual(result["verdict"], "unmet")
        self.assertEqual(result["reason"], "orphan_blob_dictionary_entry")


if __name__ == "__main__":
    unittest.main()
