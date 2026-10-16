"""Synthetic preservation contracts; no product/storage/provider execution."""
import copy
from contextlib import closing
import base64
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import preservation_plan as p


def row(table, key, **fields):
    return {"id": f"{table}:{key}", "space_id": "household", "deleted_at": None,
            "updated_at": "2026-10-10T00:00:00Z", **fields}


def snapshot(tables):
    return {"backend": "elephant-db", "version": 1,
            "exported_at": "2026-10-10T00:00:00Z", "tables": tables}


def change(table, before, after):
    fields = sorted(k for k in set(before or {}) | set(after or {})
                    if (k in (before or {})) != (k in (after or {}))
                    or p.canonical((before or {}).get(k)) != p.canonical((after or {}).get(k)))
    return {"table": table, "key": (before or after)["id"],
            "class": "graph", "kind": p.change_kind(before, after),
            "flags": [], "changed_fields": fields, "dependency_rank": 1,
            "updated_at": {"s0": (before or {}).get("updated_at"), "s1": (after or {}).get("updated_at")},
            "deleted_at": {"s0": (before or {}).get("deleted_at"), "s1": (after or {}).get("deleted_at")},
            "row_sha256": {"s0": None if before is None else p.value_hash(before),
                           "s1": None if after is None else p.value_hash(after)},
            "before": before, "after": after}


class PreservationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.doc = row("doc", "one", title="Títle", text=" raw\ntext 🐘\n",
                       security={"level": 2}, provenance={"sources": []})
        self.graph = row("entity", "one", name="original")
        self.tables = {"doc": [self.doc], "entity": [self.graph]}

    def tearDown(self):
        self.temp.cleanup()

    def build(self, tables=None, mappings=None, suffix=""):
        tables = tables or self.tables
        mappings = mappings or [{"old_doc_id": doc["id"], "class": "source_less",
                                  "content_sha256": p.value_hash({"title": doc["title"], "text": doc["text"]})}
                                 for doc in tables["doc"]]
        snap = self.root / ("snapshot" + suffix)
        ids = self.root / ("idmap" + suffix)
        snap.write_bytes(p.canonical(snapshot(tables)))
        ids.write_bytes(b"".join(p.canonical(item) + b"\n" for item in mappings))
        out = self.root / ("plan" + suffix)
        plan = p.inventory(snap, ids, out, space="household",
                           snapshot_sha256=p.file_sha(snap), idmap_sha256=p.file_sha(ids))
        return out, plan

    def run_delta(self, out, plan, tables, changes, suffix=""):
        after = self.root / ("after" + suffix)
        diff = self.root / ("diff" + suffix)
        after.write_bytes(p.canonical(snapshot(tables)))
        diff.write_bytes(b"".join(p.canonical(item) + b"\n" for item in changes))
        return p.delta(out / "inventory.sqlite", diff, after, self.root / ("delta" + suffix),
                       before_snapshot_sha256=plan["snapshot_sha256"],
                       after_snapshot_sha256=p.file_sha(after), changes_sha256=p.file_sha(diff),
                       baseline_sha256=plan["inventory_sha256"])

    def test_archive_mode_is_explicit_and_bound_to_inventory_and_delta(self):
        work = row("work_item", "one", status="running")
        tables = {**self.tables, "work_item":[work], "work_edge":[]}
        snap=self.root/"archive-source";ids=self.root/"archive-idmap"
        snap.write_bytes(p.canonical(snapshot(tables)))
        ids.write_bytes(p.canonical({"old_doc_id":self.doc["id"],"class":"source_less",
            "content_sha256":p.value_hash({"title":self.doc["title"],"text":self.doc["text"]})})+b"\n")
        out=self.root/"archive-plan"
        plan=p.inventory(snap,ids,out,space="household",snapshot_sha256=p.file_sha(snap),
            idmap_sha256=p.file_sha(ids),operational_disposition="managed_archive_v1")
        proof=plan["operational_archive_inventory"]
        self.assertEqual(proof["tables"],{"work_edge":0,"work_item":1})
        self.assertNotIn("work_deadletter",proof["tables"])
        self.assertEqual(proof["rows"],1)
        self.assertEqual(plan["runtime_requirements"][0]["disposition"],"archive_inert")
        self.assertFalse(plan["runtime_requirements"][0]["verified"])
        with closing(sqlite3.connect(out/"inventory.sqlite")) as db:
            raw=db.execute("SELECT lexical FROM raw_rows WHERE t='work_item'").fetchone()[0]
            digest=p.hashlib.sha256(b"recall-operational-archive-rows-v1\0")
            digest.update(p.canonical(["work_edge",0]));digest.update(p.canonical(["work_item",1]))
            digest.update(p.canonical(["work_item",work["id"],p.sha(raw),len(raw)]))
            self.assertEqual(proof["ordered_inventory_sha256"],digest.hexdigest())
            emitted=p.validate_change(change("work_item",work,None),db,"household")
            self.assertEqual(emitted["disposition"],"guarded_archive_row_change")
            self.assertTrue(emitted["destructive_intent_required"])
            self.assertFalse(emitted["physical_erase_authorized"])
            self.assertFalse(emitted["target_guard"]["target_native_write"])
            graph=p.validate_change(change("entity",self.graph,{**self.graph,"name":"changed"}),db,"household")
            self.assertEqual(graph["disposition"],"guarded_target_row_change")
        result=self.run_delta(out,plan,{**tables,"work_item":[]},[change("work_item",work,None)])
        self.assertEqual(result["operational_disposition"],"managed_archive_v1")

    def test_native_default_and_unknown_archive_mode_never_skip_work(self):
        work=row("work_item","one",status="running")
        out,plan=self.build({**self.tables,"work_item":[work]})
        self.assertEqual(plan["operational_disposition"],"native")
        self.assertIsNone(plan["operational_archive_inventory"])
        self.assertEqual(plan["runtime_requirements"][0]["kind"],"historical_work_hold_v1")
        with closing(sqlite3.connect(out/"inventory.sqlite")) as db:
            self.assertEqual(p.validate_change(change("work_item",work,None),db,"household")["disposition"],"guarded_target_row_change")
        for mode in [None,"archive",{"work_item":"skip"}]:
            with self.assertRaises(p.Refused):p.operational_mode(mode)

    def test_emitted_delta_json_matches_required_rust_planned_change_contract(self):
        # Actual serialized producer output must supply the strict Rust fields.
        out, plan = self.build()
        after = {**self.graph, "name": "changed"}
        self.run_delta(out, plan, {**self.tables, "entity": [after]},
                       [change("entity", self.graph, after)])
        emitted = json.loads((self.root / "delta" / "changes.jsonl").read_bytes())
        expected = {"table", "key", "kind", "before_sha256", "after_sha256",
                    "destructive_intent_required", "physical_erase_authorized",
                    "disposition", "advisory_metadata", "target_guard", "before",
                    "after", "operation_id"}
        self.assertEqual(set(emitted), expected)
        self.assertEqual(emitted["disposition"], "guarded_target_row_change")
        self.assertIs(emitted["physical_erase_authorized"], False)
        self.assertIs(emitted["destructive_intent_required"], False)
        self.assertEqual(emitted["before"], self.graph)
        self.assertEqual(emitted["after"], after)
        self.assertEqual(emitted["before_sha256"], p.value_hash(self.graph))
        self.assertEqual(emitted["after_sha256"], p.value_hash(after))

    def test_global_maintenance_delta_is_retained_without_target_schema_writes(self):
        before={"id":"migration_history:fake_embedding_purge_v1","name":"fake_embedding_purge_v1","checksum":"fake-embedding-ramp-v1","applied_at":"2026-10-10T00:00:00Z"}
        after={**before,"applied_at":"2026-10-11T00:00:00Z"}
        out,plan=self.build({**self.tables,"migration_history":[before]})
        result=self.run_delta(out,plan,{**self.tables,"migration_history":[after]},[change("migration_history",before,after)])
        self.assertTrue(result["after_snapshot_completeness_verified"])
        saved=json.loads((self.root/"delta"/"changes.jsonl").read_text())
        self.assertEqual(saved["disposition"],"retain_source_control_only")
        self.assertEqual(saved["target_guard"],{"kind":"source_provenance_only","target_schema_write":False})
        self.assertEqual(saved["before"],before)
        self.assertEqual(saved["after"],after)
        for bad in [{**after,"space_id":"household"},{**after,"checksum":"unknown"},{**after,"applied_at":"2026-99-11T00:00:00Z"}]:
            with self.assertRaises(p.Refused):
                with closing(sqlite3.connect(out/"inventory.sqlite")) as db:
                    p.validate_change(change("migration_history",before,bad),db,"household")

    def test_global_retained_control_create_delete_and_stale_original(self):
        receipt={"id":"migration_history:fake_embedding_purge_v1","name":"fake_embedding_purge_v1","checksum":"fake-embedding-ramp-v1","applied_at":"2026-10-10T00:00:00Z"}
        out,plan=self.build({**self.tables,"migration_history":[]})
        self.run_delta(out,plan,{**self.tables,"migration_history":[receipt]},[change("migration_history",None,receipt)])
        out2,plan2=self.build({**self.tables,"migration_history":[receipt]},suffix="remove")
        self.run_delta(out2,plan2,{**self.tables,"migration_history":[]},[change("migration_history",receipt,None)],suffix="remove")
        with closing(sqlite3.connect(out2/"inventory.sqlite")) as db:
            stale={**receipt,"applied_at":"2026-10-09T00:00:00Z"}
            with self.assertRaisesRegex(p.Refused,"delta_baseline_mismatch"):
                p.validate_change(change("migration_history",stale,None),db,"household")

    def test_inline_media_is_bounded_verified_and_path_not_opened(self):
        raw = bytes(range(256)) * 600
        media = row("media_asset", "one", payload_base64=base64.b64encode(raw).decode(),
                    size_bytes=len(raw), sha256=p.sha(raw), storage_path="../../not-opened")
        out, plan = self.build({**self.tables, "media_asset": [media]})
        stats = plan["media_preservation"]
        self.assertTrue(stats["self_contained"])
        self.assertEqual(stats["decoded_inline_bytes"], len(raw))
        self.assertEqual(stats["inline_rows"], 1)
        with sqlite3.connect(out / "inventory.sqlite") as db:
            retained = db.execute("SELECT lexical FROM raw_rows WHERE t='media_asset'").fetchone()[0]
        self.assertEqual(json.loads(retained)["payload_base64"], media["payload_base64"])

    def test_inline_media_changed_digest_and_length_refused(self):
        original = row("media_asset", "one", payload_base64="YQ==", size_bytes=1,
                       sha256=p.sha(b"a"), storage_path="media_asset/one")
        for field, value in [("sha256", p.sha(b"b")), ("size_bytes", 2),
                             ("payload_base64", "YQ=\n"), ("payload_base64", "YR==")]:
            with self.subTest(field=field, value=value):
                with self.assertRaises(p.Refused):
                    p.validate_media({**original, field: value})
        with self.assertRaisesRegex(p.Refused, "size_bound"):
            p.validate_media(original, max_decoded_bytes=0)

    def test_external_media_requires_availability_and_delta_never_claims_self_contained(self):
        out, plan = self.build()
        media = row("media_asset", "one", payload_base64=None, size_bytes=10,
                    sha256=p.sha(b"0123456789"), storage_path="media_asset/one")
        target = {**self.tables, "media_asset": [media]}
        result = self.run_delta(out, plan, target, [change("media_asset", None, media)])
        self.assertFalse(result["media_preservation"]["self_contained"])
        self.assertEqual(result["runtime_blockers"], ["external_media_availability_required"])
        _, external = self.build(target, suffix="external")
        self.assertEqual(external["runtime_blockers"], ["external_media_availability_required"])

    def test_original_bytes_and_hash_domains(self):
        out, plan = self.build()
        item = json.loads((out / "documents.jsonl").read_text())
        self.assertEqual(item["original_text_sha256"], p.sha(self.doc["text"].encode()))
        self.assertNotEqual(item["doc_content_sha256"], item["original_text_sha256"])
        self.assertIsNone(item["accepted_request_fingerprint"])
        self.assertFalse(plan["publication_certified"])
        self.assertEqual(item["external_source_id"], "hc-legacy-sha256-" + p.sha(b"doc:one")[:40])

    def test_no_200k_truncation(self):
        self.doc["text"] = "é" * 200001
        out, _ = self.build()
        item = json.loads((out / "documents.jsonl").read_text())
        self.assertEqual(item["original_text_utf8_bytes"], 400002)

    def test_multiple_documents_one_derived_key(self):
        second = {**self.doc, "id": "doc:two"}
        mappings = [{"old_doc_id": d["id"], "class": "derived", "source_key": "shared",
                     "content_sha256": p.value_hash({"title": d["title"], "text": d["text"]})}
                    for d in [self.doc, second]]
        out, plan = self.build({"doc": [self.doc, second]}, mappings)
        group = json.loads((out / "groups.jsonl").read_text())
        self.assertEqual(plan["groups"], 1)
        self.assertEqual(group["documents"], 2)
        self.assertEqual(group["external_source_id"], "hc-src-shared")
        self.assertEqual(len((out / "documents.jsonl").read_text().splitlines()), 2)

    def test_idmap_mismatch(self):
        with self.assertRaisesRegex(p.Refused, "content_mismatch"):
            self.build(mappings=[{"old_doc_id": "doc:one", "class": "source_less", "content_sha256": "0" * 64}])

    def test_duplicate_identity(self):
        with self.assertRaisesRegex(p.Refused, "duplicate_identity"):
            self.build({"doc": [self.doc, self.doc]})

    def test_modern_authority_is_blocker_not_silently_dropped(self):
        _, plan = self.build({**self.tables, "source_head": [row("source_head", "one")]})
        self.assertIn("journal_aware_authority:source_head", plan["runtime_blockers"])
        self.assertEqual(plan["tables"]["source_head"], 1)

    def test_stream_chunk_boundaries_unicode_and_numbers(self):
        path = self.root / "chunked"
        path.write_bytes(p.canonical(snapshot({"doc": [self.doc], "test": [{"id": "test:1", "v": 12345.67}]})))
        stream = p.JsonStream(path, chunk_bytes=1)
        try:
            result = list(stream.snapshot())
            self.assertEqual(result[0][1], self.doc)
            self.assertEqual(result[1][1]["v"], 12345.67)
            self.assertEqual(stream.digest.hexdigest(), p.file_sha(path))
        finally:
            stream.close()

    def test_stream_rejects_duplicate_keys_trailing_and_nan(self):
        for i, raw in enumerate([b'{"backend":"elephant-db","backend":"elephant-db"}',
                                 p.canonical(snapshot({})) + b'x',
                                 b'{"backend": NaN}']):
            path = self.root / str(i)
            path.write_bytes(raw)
            stream = p.JsonStream(path, chunk_bytes=2)
            try:
                with self.assertRaises(p.Refused):
                    list(stream.snapshot())
            finally:
                stream.close()

    def test_graph_only_delta_and_stale_target_guard(self):
        out, plan = self.build()
        after = {**self.graph, "name": "manual edit"}
        delta = change("entity", self.graph, after)
        result = self.run_delta(out, plan, {**self.tables, "entity": [after]}, [delta])
        self.assertEqual(result["changes"], 1)
        self.assertTrue(result["after_snapshot_completeness_verified"])
        self.assertFalse(result["target_cas_verified"])
        db = sqlite3.connect(out / "inventory.sqlite")
        validated = p.validate_change(delta, db, "household")
        p.validate_target_guard(validated, self.graph)
        with self.assertRaisesRegex(p.Refused, "before_image_conflict"):
            p.validate_target_guard(validated, after)
        db.close()

    def test_missing_graph_delta_rejected(self):
        out, plan = self.build()
        with self.assertRaisesRegex(p.Refused, "incomplete_or_wrong"):
            self.run_delta(out, plan, {**self.tables, "entity": [{**self.graph, "name": "changed"}]}, [])

    def test_false_after_snapshot_and_missing_removal(self):
        out, plan = self.build()
        with self.assertRaisesRegex(p.Refused, "missing_removed"):
            self.run_delta(out, plan, {"doc": [self.doc]}, [])

    def test_create_delete_restore_remove_preserve_tombstone_intent(self):
        out, plan = self.build()
        db = sqlite3.connect(out / "inventory.sqlite")
        new = row("entity", "new", name="new")
        self.assertEqual(p.validate_change(change("entity", None, new), db, "household")["target_guard"], {"kind": "absent"})
        for after in [None, {**self.graph, "deleted_at": "2026-10-11T00:00:00Z"}]:
            result = p.validate_change(change("entity", self.graph, after), db, "household")
            self.assertTrue(result["destructive_intent_required"])
            self.assertFalse(result["physical_erase_authorized"])
        db.close()
        dead = {**self.graph, "deleted_at": "2026-10-11T00:00:00Z"}
        out2, _ = self.build({**self.tables, "entity": [dead]}, suffix="2")
        db = sqlite3.connect(out2 / "inventory.sqlite")
        self.assertEqual(p.validate_change(change("entity", dead, self.graph), db, "household")["kind"], "restored")
        db.close()

    def test_duplicate_delta_and_wrong_hash_refused(self):
        out, plan = self.build()
        after = {**self.graph, "name": "changed"}
        item = change("entity", self.graph, after)
        with self.assertRaisesRegex(p.Refused, "duplicate_delta"):
            self.run_delta(out, plan, {**self.tables, "entity": [after]}, [item, item])
        item["row_sha256"]["s0"] = "0" * 64
        with self.assertRaisesRegex(p.Refused, "row_hash"):
            self.run_delta(out, plan, {**self.tables, "entity": [after]}, [item], suffix="2")

    def test_delta_determinism_and_altered_replay(self):
        out, plan = self.build()
        after = {**self.graph, "name": "changed"}
        item = change("entity", self.graph, after)
        a = self.run_delta(out, plan, {**self.tables, "entity": [after]}, [item])
        b = self.run_delta(out, plan, {**self.tables, "entity": [after]}, [item], suffix="2")
        self.assertEqual(a["plan_id"], b["plan_id"])
        forged = copy.deepcopy(item)
        forged["before"]["name"] = "forged"
        forged["row_sha256"]["s0"] = p.value_hash(forged["before"])
        with self.assertRaisesRegex(p.Refused, "baseline_mismatch"):
            self.run_delta(out, plan, {**self.tables, "entity": [after]}, [forged], suffix="3")

    def test_utf8_limit_counts_bytes_not_characters(self):
        path = self.root / "unicode-limit"
        raw = p.canonical({"text": "🐘" * 25})
        path.write_bytes(raw)
        for limit, succeeds in [(len(raw), True), (len(raw) - 1, False), (40, False)]:
            stream = p.JsonStream(path, max_value_bytes=limit, chunk_bytes=1)
            try:
                if succeeds:
                    self.assertEqual(stream.value(), {"text": "🐘" * 25})
                else:
                    with self.assertRaisesRegex(p.Refused, "json_value_limit"):
                        stream.value()
            finally:
                stream.close()

    def test_numeric_token_boundaries(self):
        for number in ["-1.25e+12", "0.125", "-0.0", "12345678901234567890"]:
            path = self.root / "number"
            path.write_text(number + ",")
            stream = p.JsonStream(path, chunk_bytes=1)
            try:
                self.assertEqual(p.canonical(stream.value()), p.canonical(json.loads(number)))
                self.assertEqual(stream.peek(), ",")
            finally:
                stream.close()

    def test_array_tokens_and_envelope_fail_closed(self):
        valid = p.canonical(snapshot({"doc": [self.doc]}))
        raw_doc = p.canonical(self.doc)
        cases = [valid.replace(raw_doc + b"]", raw_doc + b",]"),
                 valid.replace(raw_doc, raw_doc + raw_doc),
                 valid.replace(b'"version":1', b'"version":1,"version":1'),
                 valid.replace(b'"version":1', b'"version":true'),
                 valid.replace(b'"version":1', b'"version":2'),
                 valid.replace(b'"tables":{', b'"tables":{"doc":[],', 1),
                 valid.replace(b'"exported_at":', b'"unknown":'),
                 valid[:-1], b'{"backend":"elephant-db","version":1,"exported_at":null,"tables":{}}']
        for index, raw in enumerate(cases):
            with self.subTest(index=index):
                path = self.root / "invalid"
                path.write_bytes(raw)
                stream = p.JsonStream(path, chunk_bytes=3)
                try:
                    with self.assertRaises(p.Refused):
                        list(stream.snapshot())
                finally:
                    stream.close()

    def test_empty_tables_and_authorities_retained(self):
        out, plan = self.build({**self.tables, "assertion": [], "canonical_epoch": [],
                                "native_read_fence": [], "provider_call_receipt": []})
        self.assertEqual(plan["tables"]["assertion"], 0)
        for table in ["canonical_epoch", "native_read_fence", "provider_call_receipt"]:
            self.assertIn("journal_aware_authority:" + table, plan["runtime_blockers"])
        with self.assertRaisesRegex(p.Refused, "baseline_has_runtime_blockers"):
            self.run_delta(out, plan, self.tables, [])

    def test_numeric_type_changes_are_real_changes(self):
        self.graph["value"] = 1
        out, plan = self.build()
        after = {**self.graph, "value": 1.0}
        result = self.run_delta(out, plan, {**self.tables, "entity": [after]},
                                [change("entity", self.graph, after)])
        self.assertEqual(result["changes"], 1)
        self.assertEqual(result["canonical_hash_contract"]["id"], p.HASH_ALGORITHM)
        self.assertNotEqual(p.value_hash(1), p.value_hash(1.0))
        self.assertNotEqual(p.value_hash(-0.0), p.value_hash(0.0))
        self.assertNotEqual(p.value_hash(True), p.value_hash(1))

    def test_unknown_empty_after_table_and_declaration_changes(self):
        out, plan = self.build()
        with self.assertRaisesRegex(p.Refused, "delta_unsupported_table"):
            self.run_delta(out, plan, {**self.tables, "unknown_authority": []}, [])
        result = self.run_delta(out, plan, {**self.tables, "assertion": []}, [], suffix="known")
        self.assertEqual(result["table_declaration_changes"], {"added": ["assertion"], "removed": []})

    def test_delta_metadata_is_typed_and_never_authority(self):
        out, _ = self.build()
        db = sqlite3.connect(out / "inventory.sqlite")
        try:
            item = change("entity", self.graph, {**self.graph, "name": "new"})
            for field, value in [("class", "erase_authorized"), ("flags", ["bypass"]),
                                 ("dependency_rank", True)]:
                with self.subTest(field=field), self.assertRaises(p.Refused):
                    p.validate_change({**item, field: value}, db, "household")
            self.assertFalse(p.validate_change(item, db, "household")["physical_erase_authorized"])
        finally:
            db.close()

    def test_lexical_spool_retains_original_numeric_and_whitespace_bytes(self):
        snap = self.root / "lexical-snapshot"
        ids = self.root / "lexical-idmap"
        doc = p.canonical(self.doc)
        raw = b'{ "id" : "entity:one", "space_id":"household", "v":1.2300e+00 }'
        snap.write_bytes(b'{"backend":"elephant-db","version":1,"exported_at":"2026-10-10T00:00:00Z","tables":{"doc":[' + doc + b'],"entity":[' + raw + b']}}')
        ids.write_bytes(p.canonical({"old_doc_id": "doc:one", "class": "source_less", "content_sha256": p.value_hash({"title": self.doc["title"], "text": self.doc["text"]})}) + b'\n')
        out = self.root / "lexical-plan"
        plan = p.inventory(snap, ids, out, space="household", snapshot_sha256=p.file_sha(snap), idmap_sha256=p.file_sha(ids))
        db = sqlite3.connect(out / "inventory.sqlite")
        try:
            actual = db.execute("SELECT lexical,lexical_sha256 FROM raw_rows WHERE t='entity'").fetchone()
            self.assertEqual(actual, (raw, p.sha(raw)))
            self.assertNotEqual(actual[1], db.execute("SELECT h FROM rows WHERE t='entity'").fetchone()[0])
            self.assertEqual(plan["spool_schema"], "recall-original-lexical-row-spool-v1")
            self.assertEqual(plan["snapshot_header"], {"backend":"elephant-db", "version":1, "exported_at":"2026-10-10T00:00:00Z"})
            self.assertEqual(p.metadata(db, "snapshot_header"), plan["snapshot_header"])
        finally:
            db.close()

    def test_reverse_reference_paths_do_not_infer_ownership(self):
        self.doc["provenance"] = {"sources": ["source:original"]}
        self.doc["metadata"] = {"entity:key": "doc:second", "literal": "entity:untrusted"}
        self.doc["text"] = "literal text referring to entity:one"
        out, plan = self.build()
        db = sqlite3.connect(out / "inventory.sqlite")
        refs = db.execute("SELECT path,ref_table,ref_id FROM reference_candidates WHERE row_id='doc:one' ORDER BY path").fetchall()
        self.assertIn(("/provenance/sources/0", "source", "source:original"), refs)
        self.assertIn(("/metadata/entity:key/@key", "entity", "entity:key"), refs)
        self.assertIn(("/metadata/entity:key", "doc", "doc:second"), refs)
        self.assertNotIn(("/id", "doc", "doc:one"), refs)
        self.assertFalse(any(path == "/text" for path, _, _ in refs))
        self.assertFalse(plan["reference_index"]["typed_membership_verified"])
        self.assertEqual(plan["reference_index"], p.metadata(db, "reference_index"))
        db.close()

    def test_reverse_index_shared_entity_does_not_merge_sources(self):
        a = row("assertion", "a", subject="entity:shared", evidence={"node":"doc_node:a"})
        b = row("assertion", "b", subject="entity:shared", evidence={"node":"doc_node:b"})
        out, _ = self.build({**self.tables, "assertion":[a,b]})
        db = sqlite3.connect(out / "inventory.sqlite")
        refs = db.execute("SELECT row_id FROM reference_candidates WHERE ref_table='doc_node' AND ref_id='doc_node:a'").fetchall()
        self.assertEqual(refs, [("assertion:a",)])
        shared = db.execute("SELECT row_id FROM reference_candidates WHERE ref_table='entity' AND ref_id='entity:shared' ORDER BY row_id").fetchall()
        self.assertEqual(shared, [("assertion:a",), ("assertion:b",)])
        plan = db.execute("EXPLAIN QUERY PLAN SELECT row_id FROM reference_candidates WHERE ref_table='entity' AND ref_id='entity:shared'").fetchall()
        self.assertTrue(any("reverse_references" in str(step) for step in plan))
        db.close()

    def test_reference_candidates_keep_escaped_paths_and_repeated_links(self):
        refs = list(p.reference_candidates({"a/b~c":["doc:a", "doc:a"], "text":"http://example.test"}))
        self.assertIn(("/a~1b~0c/0", "doc", "doc:a"), refs)
        self.assertIn(("/a~1b~0c/1", "doc", "doc:a"), refs)
        # Even a URI-shaped lexical candidate is not accepted typed authority.
        self.assertIn(("/text", "http", "http://example.test"), refs)

    def test_python_raw_document_fixture_regenerates_exact_hash_domains(self):
        fixture = json.loads((Path(__file__).parent / "fixtures/python-raw-documents-v1.json").read_text())
        self.assertTrue(fixture["synthetic"])
        rows = [case["original"] for case in fixture["cases"]]
        snap = self.root / "interop-snapshot"
        ids = self.root / "interop-idmap"
        snap.write_bytes(p.canonical(snapshot({"doc":rows})))
        ids.write_bytes(b"".join(p.canonical({"old_doc_id":doc["id"],"class":"source_less","content_sha256":p.value_hash({"title":doc["title"],"text":doc["text"]})}) + b"\n" for doc in rows))
        plan = p.inventory(snap, ids, self.root / "interop-plan", space="history-test", snapshot_sha256=p.file_sha(snap), idmap_sha256=p.file_sha(ids))
        for field in ("snapshot_sha256", "idmap_sha256", "documents_sha256"):
            self.assertEqual(plan[field], fixture[field])
        actual = {item["legacy_doc_id"]:item for _,item in p.jsonl(self.root / "interop-plan/documents.jsonl")}
        for case in fixture["cases"]:
            self.assertEqual(p.canonical(case["original"]), case["original_lexical"].encode())
            self.assertEqual(p.sha(case["original_lexical"].encode()), case["original_lexical_sha256"])
            self.assertEqual(actual[case["original"]["id"]], case["declaration"])
        self.assertNotEqual(fixture["cases"][0]["declaration"]["provenance_sha256"], fixture["cases"][1]["declaration"]["provenance_sha256"])

    def fresh_inventory(self, docs, keys, suffix="", **overrides):
        ledger = self.root / ("ledger"+suffix+".sqlite")
        conn = sqlite3.connect(ledger)
        conn.execute("CREATE TABLE decisions(doc_key TEXT,keep INTEGER)")
        conn.executemany("INSERT INTO decisions VALUES (?,1)", [(key,) for key in keys])
        conn.commit()
        conn.close()
        snap = self.root / ("fresh"+suffix+".json")
        snap.write_bytes(p.canonical(snapshot({"doc":docs})))
        opts = dict(space="household", snapshot_sha256=p.file_sha(snap), ledger_backup=ledger,
                    ledger_sha256=p.file_sha(ledger), ledger_cut_order="store_cut_then_ledger_backup")
        opts.update(overrides)
        out = self.root / ("fresh-plan"+suffix)
        return out, p.inventory(snap, None, out, **opts)

    def test_fresh_ledger_generates_exact_stable_grouped_identity(self):
        docs = [{**self.doc,"id":"doc:a","metadata":{"source_doc_key":"shared/key"}},
                {**self.doc,"id":"doc:b","metadata":{"source_index_doc_key":"shared/key"}},
                {**self.doc,"id":"doc:c","metadata":{"source_doc_key":"missing"}}]
        out, plan = self.fresh_inventory(docs, ["shared/key", "later-extra"])
        maps = [item for _,item in p.jsonl(out / "inventory.idmap.jsonl")]
        self.assertEqual([m["class"] for m in maps], ["derived","derived","source_less"])
        self.assertEqual(maps[2]["reason"], "source_key_not_kept_in_ledger")
        self.assertEqual(plan["groups"], 2)
        self.assertEqual(plan["idmap_sha256"], p.file_sha(out / "inventory.idmap.jsonl"))
        self.assertEqual(plan["identity_input"]["ledger"]["keys"], 2)
        self.assertFalse(plan["identity_input"]["ledger"]["cut_order_verified_by_planner"])
        payloads = {item["legacy_doc_id"]:item for _,item in p.jsonl(out / "documents.jsonl")}
        self.assertEqual(payloads["doc:a"]["external_source_id"], payloads["doc:b"]["external_source_id"])
        self.assertEqual(payloads["doc:a"]["external_source_id"], p.source_id("hc-src-", "shared/key"))
        self.assertEqual(payloads["doc:c"]["external_source_id"], p.source_id("hc-legacy-", "doc:c"))

    def test_fresh_mapping_pinned_classification_cases(self):
        cases = [(None,"no_metadata"), ({"source_doc_keys":["kept"]},"multi_source_packet"),
                 ({},"no_source_key"), ({"source_doc_key":"kept","source_index_doc_key":"other"},"conflicting_source_keys"),
                 ({"source_doc_key":"missing"},"source_key_not_kept_in_ledger"),
                 ({"source_doc_key":"kept","source_index_doc_key":"kept"},"conveyor_ledger_keep")]
        docs = [{**self.doc,"id":f"doc:{i}","metadata":metadata} for i,(metadata,_) in enumerate(cases)]
        out, _ = self.fresh_inventory(docs, ["kept"])
        maps = [item for _,item in p.jsonl(out / "inventory.idmap.jsonl")]
        self.assertEqual([item["reason"] for item in maps], [reason for _,reason in cases])

    def test_fresh_ledger_refuses_unbound_wrong_order_and_sidecars(self):
        for i,opts in enumerate([{"ledger_sha256":"0"*64}, {"ledger_cut_order":"ledger_first"}]):
            with self.assertRaises(p.Refused):
                self.fresh_inventory([self.doc], [], str(i), **opts)
        ledger = self.root / "sidecar.sqlite"
        conn = sqlite3.connect(ledger)
        conn.execute("CREATE TABLE decisions(doc_key TEXT, keep INTEGER)")
        conn.commit(); conn.close()
        for suffix in ("-wal", "-journal", "-shm"):
            sidecar = Path(str(ledger)+suffix); sidecar.write_bytes(b"unbound")
            with self.assertRaisesRegex(p.Refused, "sidecar_unbound"):
                p.load_keep_backup(sqlite3.connect(":memory:"), ledger, p.file_sha(ledger), "store_cut_then_ledger_backup", 100)
            sidecar.unlink()

    def test_fresh_ledger_wal_header_refuses_even_without_sidecars(self):
        ledger = self.root / "wal.sqlite"
        conn = sqlite3.connect(ledger)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE decisions(doc_key TEXT,keep INTEGER)")
        conn.commit(); conn.close()
        self.assertFalse(Path(str(ledger)+"-wal").exists())
        with self.assertRaisesRegex(p.Refused, "wal_header_unbound"):
            p.load_keep_backup(sqlite3.connect(":memory:"), ledger, p.file_sha(ledger), "store_cut_then_ledger_backup", 100)

    def test_fresh_ledger_rejects_duplicate_and_oversized_keys(self):
        with self.assertRaisesRegex(p.Refused, "duplicate_identity"):
            self.fresh_inventory([self.doc], ["same","same"], "duplicate")
        with self.assertRaisesRegex(p.Refused, "key_shape_or_bound"):
            self.fresh_inventory([self.doc], ["é"*101], "size", max_value_bytes=200)

    def test_fresh_original_space_identity_is_never_remapped(self):
        doc = {**self.doc,"space_id":"default"}
        out, plan = self.fresh_inventory([doc], [], space="default")
        self.assertEqual(plan["space_id"], "default")
        db = sqlite3.connect(out / "inventory.sqlite")
        self.assertEqual(db.execute("SELECT space FROM rows WHERE t='doc'").fetchone(), ("default",))
        raw = db.execute("SELECT lexical FROM raw_rows WHERE t='doc'").fetchone()[0]
        self.assertEqual(json.loads(raw)["space_id"], "default")
        self.assertEqual(db.execute("SELECT h FROM rows WHERE t='doc'").fetchone()[0], p.value_hash(doc))
        db.close()

    def test_explicit_target_mapping_keeps_original_hashes_and_rejects_collision(self):
        doc={**self.doc,"space_id":"default"}
        out,plan=self.fresh_inventory([doc],[],space="default",target_space="household")
        self.assertEqual((plan["source_space_id"],plan["target_space_id"]),("default","household"))
        db=sqlite3.connect(out/"inventory.sqlite")
        self.assertEqual(p.metadata(db,"target_space_id"),"household")
        self.assertEqual(db.execute("SELECT h,space FROM rows WHERE t='doc'").fetchone(),(p.value_hash(doc),"default"))
        db.close()
        with self.assertRaisesRegex(p.Refused,"target_space_collision"):
            self.fresh_inventory([doc,{**self.doc,"id":"doc:target"}],[],"collision",space="default",target_space="household")

    def test_running_and_archived_work_require_verified_hold_not_payload_rewrite(self):
        work=row("work_item","one",status="running",attempt=4)
        archive=row("work_item_archive","two",record=row("work_item","two",status="failed",attempt=3))
        out,plan=self.build({**self.tables,"work_item":[work],"work_item_archive":[archive]})
        self.assertNotIn("running_work_item",plan["runtime_blockers"])
        requirement=plan["runtime_requirements"][0]
        self.assertEqual(requirement["kind"],"historical_work_hold_v1")
        self.assertEqual(requirement["rows"],{"work_item":1,"work_item_archive":1,"running":1})
        self.assertFalse(requirement["verified"])
        self.assertEqual(requirement["snapshot_sha256"],plan["snapshot_sha256"])
        db=sqlite3.connect(out/"inventory.sqlite")
        self.assertEqual(json.loads(db.execute("SELECT lexical FROM raw_rows WHERE t='work_item'").fetchone()[0]),work)
        self.assertEqual(p.metadata(db,"runtime_requirements"),plan["runtime_requirements"])
        db.close()

    def test_numeric_vector_skipping_preserves_mixed_reference_candidates(self):
        mixed={"vector":[0,1.5,False,None]*1000+["doc:one",{"node":"doc_node:two"},[3.0,"source:three"]],"metadata":{"entity:key":True}}
        def original_walk(row):
            stack=[("",row)]
            while stack:
                path,value=stack.pop()
                if isinstance(value,dict):
                    for key,child in value.items():
                        child_path=path+"/"+key.replace("~","~0").replace("/","~1")
                        stack.extend([(child_path,child),(child_path+"/@key",key)])
                elif isinstance(value,list):
                    stack.extend((path+"/"+str(i),child) for i,child in enumerate(value))
                elif isinstance(value,str) and path!="/id" and ":" in value:
                    table,suffix=value.split(":",1)
                    if p.TABLE.fullmatch(table) and suffix and not any(c.isspace() for c in value):
                        yield path,table,value
        self.assertEqual(set(p.reference_candidates(mixed)),set(original_walk(mixed)))
        self.assertEqual(len(list(p.reference_candidates(mixed))),4)


if __name__ == "__main__":
    unittest.main()
