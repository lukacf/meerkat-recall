"""Offline schema/lifecycle controls, never installed/provider qualification."""
import asyncio
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

spec = importlib.util.spec_from_file_location("preparation_driver", Path(__file__).with_name("workload_preparation_driver.py"))
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)


def fixture(index=0, space="fixture-household"):
    text = f"Independent short input {index}."
    source = {"space": space, "key": f"fixture/{index}", "revision": "v1", "text": text,
              "sha256": driver.sha(text.encode()), "source_url": "https://example.invalid/fixture"}
    security = {"policy_id": "fixture-policy", "level": 3, "labels": [], "handling": []}
    envelope = {"security": security, "subjects": [], "subjects_state": "unknown", "provenance": {"sources": [], "chain": []}}
    expected = {"record_security": security, "embedding_partition_model": "recall:" + "a" * 64, "input_envelope": envelope}
    rendered_text = text + "\n"
    expected["rendered_expectation"] = {"space": space, "source_key": source["key"], "revision": "v1",
        "input_sha256": source["sha256"], "input_utf8_bytes": len(text.encode()), "rendered_text": rendered_text,
        "rendered_sha256": driver.sha(rendered_text.encode()), "rendered_utf8_bytes": len(rendered_text.encode()),
        "root_span": {"start": 0, "end": len(rendered_text.encode())},
        "paragraph_spans": [{"start": 0, "end": len(text.encode())}],
        "chunk_spans": [{"start": 0, "end": len(text.encode())}]}
    request = driver.source_request(source, "elephant-db", "synthetic-nonce-0001", "2026-10-07T00:00:00Z", envelope)
    accepted = {"source_id": f"source:actual-{index}-{space}", "doc_id": f"doc:actual-{index}", "work_item_id": f"work_item:original-{index}",
                "operation": {"operation_id": f"operation-{index}", "source_id": f"source:actual-{index}-{space}",
                              "source_generation": f"generation-{index}", "space_id": space, "desired_revision": "v1", "state": "accepted"}}
    operation = {"operation_id": f"operation-{index}", "internal_source_id": accepted["source_id"],
                 "generation": f"generation-{index}", "desired_revision": "v1", "published_revision": "v1", "lifecycle": "active", "state": "complete"}
    doc = {"id": accepted["doc_id"], "source": accepted["source_id"], "space_id": space, "security": security,
           "title": source["key"], "text": rendered_text, "subjects": [], "subjects_state": "unknown"}
    record = {"id": accepted["source_id"], "space_id": space, "external_id": source["key"], "security": security,
              "current_ingest_mode": "retrieval", "lifecycle_status": "retrieval_ready", "hash": source["sha256"], "uri": source["source_url"],
              "subjects": [], "subjects_state": "unknown"}
    nodes = [{"id": f"doc_node:{index}-{role}", "space_id": space, "security": security, "revision": f"doc_revision:original-{index}",
              "span": {"start": 0, "end": len(text.encode())}, "text": text, "role": role,
              "embedding_model": expected["embedding_partition_model"], "subjects": [], "subjects_state": "unknown",
              "provenance": {"sources": [accepted["source_id"]], "chain": [{"step": "synthetic-only-lineage",
                              "input": f"doc_revision:original-{index}", "output": "synthetic-recipe"}]}}
             for role in ("paragraph", "chunk")]
    nodes.append({**nodes[0], "id": f"doc_node:{index}-document", "role": "document", "text": None,
                  "span": expected["rendered_expectation"]["root_span"], "embedding_model": None})
    observations = {"operation": operation, "head": dict(operation), "doc": doc, "source": record, "nodes": nodes,
                    "immutable_revision": {"id": f"doc_revision:original-{index}", "doc": doc["id"],
                        "space_id": space, "security": security, "subjects": [], "subjects_state": "unknown",
                        "text": rendered_text, "text_hash": driver.sha(rendered_text.encode()),
                        "status": "active", "supersedes": None, "deleted_at": None,
                        "renderer": {"name": "elephant.renderer.text", "version": "1.0.0", "config_hash": None},
                        "provenance": {"sources": [accepted["source_id"]], "chain": []},
                        "rendered_at": "2026-10-07T00:00:00Z"},
                    "pipeline": {"doc_id": accepted["doc_id"], "revision": f"doc_revision:original-{index}",
                                 "chunks": {"total": 2, "embedded": 2}, "nodes_count": 3}}
    return source, expected, request, accepted, observations


def corpus_fixture():
    spaces = ["fixture-household", "fixture-business"]
    return {"spaces": [{"id": space} for space in spaces],
            "sources": [fixture(index, space)[0] for space in spaces for index in range(25)]}


class PreparationControls(unittest.TestCase):
    def test_inventory_is_inert_and_cannot_qualify_itself(self):
        with patch.object(driver.asyncio, "open_connection", side_effect=AssertionError("no outbound")):
            value = driver.inventory()
        self.assertFalse(value["qualified"])
        self.assertEqual(value["limits"]["sources"], 50)
        self.assertEqual(value["limits"]["embedding_nodes_per_source"], 64)

    def test_typed_request_keeps_explicit_envelopes_and_original_text(self):
        source, expected, request, _, _ = fixture()
        self.assertEqual(set(request), {"source", "doc", "ingest_mode", "intake"})
        for row in (request["source"], request["doc"]):
            for key, value in expected["input_envelope"].items():
                self.assertEqual(row[key], value)
            self.assertEqual(row["created_by"], "d28-qualification")
            self.assertEqual(row["created_at"], "2026-10-07T00:00:00Z")
            self.assertEqual(row["updated_at"], "2026-10-07T00:00:00Z")
        self.assertEqual(request["doc"]["text"], source["text"])
        self.assertEqual(request["source"]["external_id"], source["key"])
        self.assertEqual(request["source"]["hash"], source["sha256"])
        self.assertIsNone(request["doc"]["source"])
        self.assertEqual(request["ingest_mode"], "retrieval")
        self.assertEqual(request["doc"]["rendition_key"], f"d28/text/{source['key']}/{source['revision']}")
        self.assertNotIn("expected_generation", request["intake"])

    def test_fresh_input_ids_are_stable_and_isolate_backend_space_and_nonce(self):
        source, expected, request, _, _ = fixture()
        args = (source, "elephant-db", "synthetic-nonce-0001", "2026-10-07T00:00:00Z", expected["input_envelope"])
        self.assertEqual(driver.source_request(*args), request)
        for changed in ((source, "surreal-db", args[2]), (source, args[1], "synthetic-nonce-0002"),
                        ({**source, "space": "fixture-business"}, args[1], args[2])):
            other = driver.source_request(*changed, args[3], args[4])
            self.assertNotEqual(other["source"]["id"], request["source"]["id"])
            self.assertNotEqual(other["doc"]["id"], request["doc"]["id"])
            self.assertNotEqual(other["intake"]["idempotency_key"], request["intake"]["idempotency_key"])

    def test_original_50_source_bytes_distribution_and_recipe_are_required(self):
        corpus = corpus_fixture()
        driver.validate_sources(corpus)
        for change in ("49", "duplicate", "space", "text", "hash", "oversize", "recipe"):
            with self.subTest(change=change):
                candidate = copy.deepcopy(corpus)
                if change == "49":
                    candidate["sources"].pop()
                elif change == "duplicate":
                    candidate["sources"][1] = candidate["sources"][0]
                elif change == "space":
                    candidate["sources"][0]["space"] = "foreign"
                else:
                    row = candidate["sources"][0]
                    if change == "text": row["text"] += " changed"
                    elif change == "hash": row["sha256"] = "0" * 64
                    elif change == "oversize": row["text"] = "x" * 513
                    else: row["text"] = "# changed recipe"
                    if change in ("oversize", "recipe"):
                        row["sha256"] = driver.sha(row["text"].encode())
                with self.assertRaises(ValueError):
                    driver.validate_sources(candidate)

    def test_acceptance_captures_actual_source_and_doc_without_guessing_private_ids(self):
        source, _, request, accepted, _ = fixture()
        self.assertNotEqual(request["doc"]["id"], accepted["doc_id"])
        self.assertNotEqual(request["source"]["id"], accepted["source_id"])
        driver.acceptance(source, accepted)
        for key in ("source_id", "space_id", "desired_revision"):
            candidate = copy.deepcopy(accepted)
            candidate["operation"][key] = "wrong"
            with self.assertRaises(ValueError): driver.acceptance(source, candidate)

    def test_public_inventory_reports_exact_mappings_and_keeps_private_proofs_unmet(self):
        source, expected, _, accepted, observations = fixture()
        result = driver.published_inventory(source, accepted, observations, expected, expected["rendered_expectation"])
        self.assertEqual(result["embeddable_node_ids"], [row["id"] for row in observations["nodes"] if row["role"] != "document"])
        self.assertNotEqual(result["source_sha256"], result["rendered_sha256"])
        self.assertTrue(result["physical_vector_inventory"].startswith("unmet"))
        self.assertTrue(result["original_private_manifest"].startswith("unmet"))
        self.assertEqual(result["original_revision_record"]["id"], observations["pipeline"]["revision"])
        self.assertEqual(result["original_revision_record"]["response_body_canonical_sha256"],
                         driver.sha(driver.encode(observations["immutable_revision"])))

    def test_immutable_revision_requires_original_mapping_rendered_bytes_and_envelope(self):
        source, expected, _, accepted, observations = fixture()
        mutations = {"missing": lambda x: x.pop("immutable_revision"),
            "parent": lambda x: x["immutable_revision"].update(doc="doc:foreign"),
            "identity": lambda x: x["immutable_revision"].update(id="doc_revision:foreign"),
            "space": lambda x: x["immutable_revision"].update(space_id="foreign"),
            "raw_text": lambda x: x["immutable_revision"].update(text=source["text"]),
            "hash": lambda x: x["immutable_revision"].update(text_hash=source["sha256"]),
            "status": lambda x: x["immutable_revision"].update(status="superseded"),
            "supersedes": lambda x: x["immutable_revision"].update(supersedes="doc_revision:prior"),
            "deleted": lambda x: x["immutable_revision"].update(deleted_at="2026-10-07T00:00:00Z"),
            "renderer": lambda x: x["immutable_revision"]["renderer"].update(version="other"),
            "config": lambda x: x["immutable_revision"]["renderer"].update(config_hash="foreign"),
            "security": lambda x: x["immutable_revision"].update(security={"policy_id": "default", "level": 0}),
            "subjects": lambda x: x["immutable_revision"].update(subjects=["entity:foreign"]),
            "lineage": lambda x: x["immutable_revision"]["provenance"].update(sources=["source:foreign"])}
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                changed = copy.deepcopy(observations)
                mutate(changed)
                with self.assertRaises(ValueError):
                    driver.published_inventory(source, accepted, changed, expected, expected["rendered_expectation"])

    def test_changed_publication_envelope_bytes_span_profile_or_counts_fail(self):
        source, expected, _, accepted, observations = fixture()
        mutations = {
            "pending": lambda x: x["operation"].update(state="pending"),
            "generation": lambda x: x["head"].update(generation="later"),
            "revision": lambda x: x["head"].update(published_revision="v2"),
            "text": lambda x: x["doc"].update(text="changed"),
            "raw_without_lf": lambda x: x["doc"].update(text=source["text"]),
            "extra_lf": lambda x: x["doc"].update(text=source["text"] + "\n\n"),
            "root_without_lf": lambda x: x["nodes"][2].update(span={"start": 0, "end": len(source["text"].encode())}),
            "chunk_includes_lf": lambda x: x["nodes"][1].update(span={"start": 0, "end": len(source["text"].encode()) + 1}),
            "source": lambda x: x["doc"].update(source="source:foreign"),
            "space": lambda x: x["source"].update(space_id="foreign"),
            "security": lambda x: x["doc"].update(security={"policy_id": "default", "level": 0}),
            "subjects": lambda x: x["doc"].update(subjects=["entity:foreign"]),
            "subject_state": lambda x: x["source"].update(subjects_state="complete"),
            "node_subject_state": lambda x: x["nodes"][0].update(subjects_state="complete"),
            "lineage_source": lambda x: x["nodes"][0]["provenance"].update(sources=["source:foreign"]),
            "lineage_revision": lambda x: x["nodes"][0]["provenance"]["chain"][0].update(input="doc_revision:foreign"),
            "node_revision": lambda x: x["nodes"][0].update(revision="doc_revision:foreign"),
            "span": lambda x: x["nodes"][0].update(span={"start": 0, "end": 99999}),
            "quote": lambda x: x["nodes"][0].update(text="invented"),
            "profile": lambda x: x["nodes"][0].update(embedding_model="recall:other"),
            "summary": lambda x: x["nodes"][0].update(role="summary"),
            "duplicate_node": lambda x: x["nodes"].append(x["nodes"][0]),
            "count": lambda x: x["pipeline"]["chunks"].update(embedded=1),
        }
        for name, mutation in mutations.items():
            with self.subTest(name=name):
                candidate = copy.deepcopy(observations)
                mutation(candidate)
                with self.assertRaises(ValueError):
                    driver.published_inventory(source, accepted, candidate, expected, expected["rendered_expectation"])

    def test_rendered_inventory_refuses_foreign_expectation_even_with_same_text(self):
        source, expected, _, accepted, observations = fixture()
        for name in ("space", "source_key", "revision", "input_sha256"):
            rendered = copy.deepcopy(expected["rendered_expectation"])
            rendered[name] = "foreign"
            with self.subTest(name=name), self.assertRaises(ValueError):
                driver.published_inventory(source, accepted, observations, expected, rendered)

    def test_configuration_comparison_allows_only_storage_epoch_and_timestamp(self):
        original = {"id": "space:original", "space_id": "fixture", "updated_at": "earlier",
                    "processing": {"generation": 1, "index_generation": 0, "active_embedding_profile_id": "approved"}}
        current = copy.deepcopy(original)
        current["updated_at"] = "later"
        current["processing"]["index_generation"] = 5
        self.assertEqual(driver.stable_configuration(original), driver.stable_configuration(current))
        current["processing"]["active_embedding_profile_id"] = "different"
        self.assertNotEqual(driver.stable_configuration(original), driver.stable_configuration(current))

    def test_gateway_receipt_requires_exact_original_phase_and_terminal_evidence(self):
        manifest = {"backend": "elephant-db", "provider_allocation_sha256": "1" * 64}
        begin = {"format": "recall-exclusive-cohort-begin/v1", "authority": "exclusive_gateway_cohort", "phase": "preparation",
                 "backend": "elephant-db", "allocation_sha256": "1" * 64, "cohort_id": 1, "baseline_sha256": "2" * 64}
        closed = {"format": "recall-exclusive-cohort-accounting/v1", "verdict": "passed", "cohort_id": 1,
                  "before_sha256": "2" * 64, "after_sha256": "3" * 64, "capture_inventory_sha256": "4" * 64,
                  "sealed_receipt_sha256": "5" * 64, "admission_closed": True, "unsettled_dispatches": 0,
                  "per_request_association": "unmet", "physical_dispatch_reservations": 50, "reserved_nano_usd": 53248000}
        driver.cohort_begin(manifest, begin)
        driver.cohort_close(begin, closed)
        for key, bad in (("phase", "measurement"), ("backend", "surreal-db"), ("allocation_sha256", "0" * 64),
                         ("baseline_sha256", ""), ("cohort_id", True), ("authority", "mock")):
            with self.subTest(begin=key), self.assertRaises(ValueError):
                driver.cohort_begin(manifest, {**begin, key: bad})
        for key, bad in (("cohort_id", 3), ("cohort_id", True), ("before_sha256", "0" * 64), ("admission_closed", False),
                         ("unsettled_dispatches", 1), ("unsettled_dispatches", False), ("physical_dispatch_reservations", 451),
                         ("reserved_nano_usd", 3000000001), ("reserved_nano_usd", 1),
                         ("sealed_receipt_sha256", ""), ("per_request_association", "inferred")):
            with self.subTest(close=key), self.assertRaises(ValueError):
                driver.cohort_close(begin, {**closed, key: bad})

    def test_typed_delta_cannot_change_corpus_allocation_or_subject_authority(self):
        manifest = {"corpus_sha256": "1" * 64, "provider_allocation_sha256": "2" * 64,
                    "preparation_contract_sha256": "3" * 64, "fixture_timestamp": "2026-10-07T00:00:00Z"}
        security = {"policy_id": "fixture-policy", "level": 3, "labels": [], "handling": []}
        loaded = {"corpus": {"spaces": [{"id": "fixture", "policy_id": "fixture-policy", "level": 3}]},
                  "preparation_contract": {"format": "recall-workload-typed-preparation/v1", "corpus_sha256": "1" * 64,
                      "top_level_fields": ["source", "doc", "ingest_mode", "intake"], "ingest_mode": "retrieval",
                      "fixture_timestamp": manifest["fixture_timestamp"], "created_by": "d28-qualification",
                      "shared_input_base": {"deleted_at": None, "subjects": [], "subjects_state": "unknown",
                                            "provenance": {"sources": [], "chain": []}},
                      "security_by_space": {"fixture": security}, "doc": {"rendition_key_rule": "d28/text/{key}/{revision}"}},
                  "typed_delta": {"format": "recall-workload-allocation-representation-delta/v1",
                      "base_allocation_sha256": "2" * 64, "preparation_contract_sha256": "3" * 64, "corpus_sha256": "1" * 64,
                      "override": {"preparation.representation": "typed Source+Doc envelope",
                                   "preparation.contract": "workload-preparation-contract.json"}}}
        driver.typed_contract(manifest, loaded)
        changes = {
            "corpus": lambda x: x["typed_delta"].update(corpus_sha256="0" * 64),
            "allocation": lambda x: x["typed_delta"].update(base_allocation_sha256="0" * 64),
            "contract": lambda x: x["typed_delta"].update(preparation_contract_sha256="0" * 64),
            "policy": lambda x: x["preparation_contract"]["security_by_space"]["fixture"].update(policy_id="default", level=0),
            "complete": lambda x: x["preparation_contract"]["shared_input_base"].update(subjects_state="complete"),
            "timestamp": lambda x: x["preparation_contract"].update(fixture_timestamp="2026-10-06T20:11:00Z"),
            "actor": lambda x: x["preparation_contract"].update(created_by="D28-qualification"),
            "extra_sync": lambda x: x["preparation_contract"]["top_level_fields"].append("sync"),
            "override": lambda x: x["typed_delta"]["override"].update(physical_dispatch_cap=9999),
        }
        for name, change in changes.items():
            candidate = copy.deepcopy(loaded)
            change(candidate)
            with self.subTest(change=name), self.assertRaises(ValueError):
                driver.typed_contract(manifest, candidate)


class PreparationLifecycleControls(unittest.IsolatedAsyncioTestCase):
    def stub(self, root):
        value = driver.Preparation.__new__(driver.Preparation)
        value.manifest = {"source_commit": "1" * 40, "artifact_sha256": "2" * 64, "binary_sha256": "3" * 64,
                          "backend": "elephant-db", "phase": "preparation", "provider_phase": "preparation", "endpoint": "http://127.0.0.1:31000",
                          "nonce": "synthetic-nonce-0001", "fixture_timestamp": "2026-10-07T00:00:00Z",
                          "corpus_sha256": "4" * 64, "contract_sha256": "5" * 64, "expires_at": "2999-01-01T00:00:00Z",
                          "preparation_contract_sha256": "6" * 64, "provider_allocation_sha256": "7" * 64,
                          "typed_delta_sha256": "8" * 64,
                          "rendered_map_sha256": "a" * 64,
                          "space_credentials": {"fixture-household": "service", "fixture-business": "service"}}
        value.root = root
        value.host, value.port = "127.0.0.1", 31000
        value.tokens = {"service": "synthetic-private-token"}
        value.loaded = {"corpus": corpus_fixture(), "contract": {}, "provider_allocation": {},
                        "expected_configurations": {space: fixture(space=space)[1] for space in ("fixture-household", "fixture-business")}}
        value.http_count = value.capture_bytes = value.posts = 0
        value.accepted, value.published = [], []
        value.provider_cohort_begin = None
        value.bridge = SimpleNamespace(authorize_provider_allocation=Mock(return_value={
                                           "format": "recall-exclusive-cohort-begin/v1", "authority": "exclusive_gateway_cohort",
                                           "phase": "preparation", "backend": "elephant-db", "allocation_sha256": "7" * 64,
                                           "baseline_sha256": "9" * 64, "cohort_id": 1}),
                                       provider_accounting=Mock(return_value={"verdict": "unmet"}))
        return value

    async def test_checks_all_heads_before_first_post_and_never_resubmits_uncertain_acceptance(self):
        value = self.stub(Path("/never-written"))
        value.configurations = AsyncMock()
        calls = []
        async def http(method, path, space, body=None):
            calls.append(method)
            if method == "POST": raise TimeoutError("acknowledgement unknown")
            return {"status": 404, "body": {"error_code": "not_found", "message": "absent", "retryable": False}}
        value.http = http
        with self.assertRaises(TimeoutError): await value.prepare()
        self.assertEqual(calls, ["GET"] * 50 + ["POST"])
        self.assertEqual(value.posts, 1)
        self.assertEqual(value.accepted, [])

    async def test_existing_last_head_prevents_every_submission(self):
        value = self.stub(Path("/never-written"))
        value.configurations = AsyncMock()
        value.http = AsyncMock(side_effect=[{"status": 404, "body": {"error_code": "not_found", "message": "absent", "retryable": False}}] * 49 + [{"status": 200}])
        with self.assertRaises(ValueError): await value.prepare()
        self.assertEqual(value.http.await_count, 50)
        self.assertEqual(value.posts, 0)

    async def test_http_retains_one_failed_response_without_retry_or_credential_capture(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "captures").mkdir()
            value = self.stub(root)
            reader = asyncio.StreamReader()
            reader.feed_data(b'HTTP/1.1 202 Accepted\r\nContent-Length: 99\r\n\r\n{"partial":true}')
            reader.feed_eof()
            writer = Mock(drain=AsyncMock(), wait_closed=AsyncMock())
            with patch.object(driver.asyncio, "open_connection", AsyncMock(return_value=(reader, writer))) as connect:
                with self.assertRaises(ValueError):
                    await value.http("POST", "/v1/sources/ingest", "fixture-household", {"original": True})
            connect.assert_awaited_once()
            writer.close.assert_called_once()
            capture = (root / "captures/0001.json").read_text()
            self.assertNotIn("synthetic-private-token", capture)
            self.assertFalse(json.loads(capture)["response"]["framing_complete"])
            intent_raw = (root / "captures/0001.request.json").read_bytes()
            self.assertNotIn(b"synthetic-private-token", intent_raw)
            self.assertEqual(json.loads(capture)["request_intent_sha256"], driver.sha(intent_raw))

    async def test_original_intent_is_private_and_persisted_before_any_connection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "captures").mkdir()
            value = self.stub(root)
            body = {"original": "literal input; no resubmission"}
            intent_path = root / "captures/0001.request.json"
            async def refused_connection(*_args, **_kwargs):
                original = json.loads(intent_path.read_bytes())
                self.assertEqual(original["request_body"], body)
                self.assertEqual(original["request_sha256"], driver.sha(driver.encode(body)))
                self.assertEqual(original["request_index"], 1)
                self.assertEqual(original["harness_attempt"], 1)
                self.assertEqual(original["credential_role"], "service")
                self.assertEqual(intent_path.stat().st_mode & 0o777, 0o600)
                self.assertNotIn("synthetic-private-token", intent_path.read_text())
                raise ConnectionRefusedError("controlled no-server condition")
            with patch.object(driver.asyncio, "open_connection", AsyncMock(side_effect=refused_connection)) as connect:
                with self.assertRaises(ConnectionRefusedError):
                    await value.http("POST", "/v1/sources/ingest", "fixture-household", body)
            connect.assert_awaited_once()
            response = json.loads((root / "captures/0001.json").read_bytes())
            self.assertEqual(response["request_intent_sha256"], driver.sha(intent_path.read_bytes()))
            self.assertEqual(response["error_type"], "ConnectionRefusedError")
            self.assertEqual(value.http_count, 1)

    async def test_intent_write_or_directory_sync_failure_prevents_dispatch(self):
        for failure in ("write", "directory-sync"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "captures").mkdir()
                value = self.stub(root)
                # Preserve the real file write in the directory-sync case; the
                # first sync persists the file and the second fails its directory.
                sync = driver.os.fsync
                calls = []
                def failed_directory_sync(descriptor):
                    calls.append(descriptor)
                    if len(calls) == 2:
                        raise OSError("controlled directory sync failure")
                    sync(descriptor)
                target = driver.helpers if failure == "write" else driver.os
                name = "private_new" if failure == "write" else "fsync"
                effect = OSError("controlled intent write failure") if failure == "write" else failed_directory_sync
                with patch.object(target, name, side_effect=effect), patch.object(driver.asyncio, "open_connection", AsyncMock()) as connect:
                    with self.assertRaises(OSError):
                        await value.http("POST", "/v1/sources/ingest", "fixture-household", {"original": True})
                connect.assert_not_awaited()
                self.assertFalse((root / "captures/0001.json").exists())

    async def test_capture_reservation_refuses_before_persisting_or_sending(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "captures").mkdir()
            value = self.stub(root)
            value.capture_bytes = driver.LIMITS["capture_bytes"] - 32
            with patch.object(driver.asyncio, "open_connection", AsyncMock()) as connect:
                with self.assertRaisesRegex(ValueError, "before dispatch"):
                    await value.http("POST", "/v1/sources/ingest", "fixture-household", {"original": True})
            connect.assert_not_awaited()
            self.assertEqual(list((root / "captures").iterdir()), [])

    async def test_begin_refusal_and_cancellation_still_close_original_cohort(self):
        for refusal in (True, False):
            with self.subTest(refusal=refusal), tempfile.TemporaryDirectory() as directory:
                value = self.stub(Path(directory))
                value.prepare = AsyncMock(side_effect=asyncio.CancelledError())
                if refusal: value.bridge.authorize_provider_allocation.side_effect = ValueError("allocation refused")
                with patch.object(driver.asyncio, "open_connection", side_effect=AssertionError("no outbound")):
                    self.assertEqual(await value.run(), 1)
                value.bridge.provider_accounting.assert_called_once()
                if refusal: value.prepare.assert_not_awaited()
                report = json.loads((value.root / "observation.json").read_text())
                self.assertFalse(report["qualified"])
                self.assertEqual(report["capture_verdict"], "failed")
                self.assertEqual(report["query_warmups"], 0)

    async def test_synchronous_stalled_close_retains_unmet_with_absolute_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            value = self.stub(Path(directory))
            value.prepare = AsyncMock(side_effect=asyncio.CancelledError())
            steps = []
            def stalled(*_args):
                steps.append("entered")
                time.sleep(1)
                steps.append("returned")
                return {"verdict": "passed"}
            value.bridge.provider_accounting.side_effect = stalled
            deadline = driver.helpers.capture.total_deadline
            with patch.object(driver.helpers.capture, "total_deadline", side_effect=lambda _seconds: deadline(0.01)) as timer:
                self.assertEqual(await value.run(), 1)
            self.assertEqual([call.args for call in timer.call_args_list], [(10,), (130,)])
            self.assertEqual(steps, ["entered"])
            report = json.loads((value.root / "observation.json").read_text())
            self.assertEqual(report["provider_accounting"]["verdict"], "unmet")
            self.assertFalse(report["qualified"])


if __name__ == "__main__":
    unittest.main()
