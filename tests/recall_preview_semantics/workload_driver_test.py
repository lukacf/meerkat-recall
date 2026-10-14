"""Offline D28 protocol/driver controls. No listener, endpoint, or provider run."""
import asyncio
import copy
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, Mock, patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("workload_driver", ROOT / "scripts/qualification/recall_workload.py")
workload = importlib.util.module_from_spec(spec)
spec.loader.exec_module(workload)


def protocol_fixture():
    """Only schema-control inputs, never D's expected product/source answers."""
    spaces = ["fixture-household", "fixture-business"]
    corpus = {"sources": [{"space": spaces[index % 2], "key": f"source-{index}"} for index in range(50)],
              "queries": [{"id": "query-" + space, "space": space, "query": "original question",
                            "wire_limit": 50, "ranking_mode": "semantic"} for space in spaces],
              "schedule": [{"id": f"caller-{worker:02}-request-{ordinal}", "caller_id": f"caller-{worker:02}", "ordinal": ordinal,
                    "space": spaces[worker % 2], "query_id": "query-" + spaces[worker % 2]}
                    for worker in range(50) for ordinal in range(4)]}
    contract = {"format": "recall-preview-workload/v2", **{key: value for key, value in workload.FIXED.items() if key not in ("callers", "source_count")},
                "concurrent_callers": 50, "spaces": spaces, "sources_per_space": {space: 25 for space in spaces},
                "schedule_sha256": workload.sha(json.dumps(corpus["schedule"], sort_keys=True, separators=(",", ":"), allow_nan=False).encode())}
    return contract, corpus


class WorkloadStaticControls(unittest.TestCase):
    def test_rendered_map_keeps_exact_original_and_projected_bytes_and_spans(self):
        texts = ["Original household note.", "Original business café note."]
        corpus = {"sources": [{"space": f"fixture-{index}", "key": f"short-{index}", "revision": "v1",
                               "text": text, "sha256": workload.sha(text.encode())} for index, text in enumerate(texts)]}
        rendered = {"format": "recall-independent-rendered-map/v1", "reviewer": "qualification (independent D lane)",
                    "execution_authorized": False, "product_acceptance": False, "corpus_sha256": "c" * 64,
                    "renderer": {"name": "elephant.renderer.text", "version": "1.0.0",
                        "source_path": "crates/pipeline/src/renderer/default.rs", "source_sha256": "a" * 64},
                    "tree": {"source_path": "crates/pipeline/src/scheduler/evidence_work_kinds.rs", "source_sha256": "b" * 64,
                        "configuration": {"max_chunk_size": 512, "chunk_overlap": 50, "create_paragraphs": True,
                            "detect_sections": False, "semantic_chunking": True, "create_summaries": False}},
                    "source_count": 2, "sources": []}
        for source in corpus["sources"]:
            raw, projected = source["text"].encode(), (source["text"] + "\n").encode()
            rendered["sources"].append({"space": source["space"], "source_key": source["key"], "revision": source["revision"],
                "input_sha256": source["sha256"], "input_utf8_bytes": len(raw), "rendered_text": projected.decode(),
                "rendered_sha256": workload.sha(projected), "rendered_utf8_bytes": len(projected),
                "root_span": {"start": 0, "end": len(projected)}, "paragraph_spans": [{"start": 0, "end": len(raw)}],
                "chunk_spans": [{"start": 0, "end": len(raw)}]})
        self.assertEqual(len(workload.rendered_map_index(rendered, corpus, "c" * 64)), 2)
        changes = {
            "missing_row": lambda x: x["sources"].pop(),
            "duplicate": lambda x: x["sources"].__setitem__(1, x["sources"][0]),
            "foreign": lambda x: x["sources"][0].update(space="foreign"),
            "revision": lambda x: x["sources"][0].update(revision="changed"),
            "raw_hash": lambda x: x["sources"][0].update(input_sha256="0" * 64),
            "raw_length": lambda x: x["sources"][0].update(input_utf8_bytes=1),
            "no_lf": lambda x: x["sources"][0].update(rendered_text=texts[0]),
            "hash": lambda x: x["sources"][0].update(rendered_sha256="0" * 64),
            "byte_length": lambda x: x["sources"][1].update(rendered_utf8_bytes=len(texts[1]) + 1),
            "root": lambda x: x["sources"][0]["root_span"].update(end=len(texts[0].encode())),
            "paragraph": lambda x: x["sources"][0]["paragraph_spans"][0].update(end=len(texts[0].encode()) + 1),
            "chunk": lambda x: x["sources"][0]["chunk_spans"][0].update(end=len(texts[0].encode()) + 1),
            "bool_span": lambda x: x["sources"][0]["chunk_spans"][0].update(start=False),
            "source": lambda x: x["renderer"].update(source_sha256="unknown"),
            "recipe": lambda x: x["tree"]["configuration"].update(create_summaries=True),
            "corpus": lambda x: x.update(corpus_sha256="d" * 64),
            "permission": lambda x: x.update(execution_authorized=True),
        }
        for name, change in changes.items():
            candidate = copy.deepcopy(rendered)
            change(candidate)
            with self.subTest(name=name), self.assertRaises(ValueError):
                workload.rendered_map_index(candidate, corpus, "c" * 64)
        with self.assertRaises(ValueError):
            workload.rendered_map_index(None, corpus, "c" * 64)

    def test_measurement_binds_typed_original_fixture_backend_and_allocation(self):
        manifest = {"provider_phase": "measurement", "nonce": "synthetic-measurement-0001",
                    "source_commit": "1" * 40, "artifact_sha256": "2" * 64, "binary_sha256": "3" * 64,
                    "backend": "elephant-db", "endpoint": "http://127.0.0.1:31000", "corpus_sha256": "4" * 64,
                    "preparation_contract_sha256": "5" * 64, "typed_delta_sha256": "6" * 64, "provider_allocation_sha256": "7" * 64,
                    "rendered_map_sha256": "8" * 64}
        fixture = {key: value for key, value in manifest.items() if key not in ("provider_phase", "nonce")}
        loaded = {"prepared_fixture": {**fixture, "qualified": True},
                  "preparation_contract": {"format": "recall-workload-typed-preparation/v1", "corpus_sha256": "4" * 64},
                  "typed_delta": {"format": "recall-workload-allocation-representation-delta/v1", "corpus_sha256": "4" * 64,
                      "preparation_contract_sha256": "5" * 64, "base_allocation_sha256": "7" * 64}}
        workload.measurement_binding(manifest, loaded)
        for name in ("source_commit", "binary_sha256", "backend", "endpoint", "corpus_sha256", "qualified",
                     "preparation_contract_sha256", "typed_delta_sha256", "provider_allocation_sha256", "rendered_map_sha256"):
            candidate = copy.deepcopy(loaded)
            candidate["prepared_fixture"][name] = False if name == "qualified" else "other"
            with self.subTest(fixture=name), self.assertRaises(ValueError):
                workload.measurement_binding(manifest, candidate)
        for name, value in (("provider_phase", "preparation"), ("provider_phase", None), ("nonce", "")):
            with self.subTest(manifest=name), self.assertRaises(ValueError):
                workload.measurement_binding({**manifest, name: value}, loaded)
        for name in ("base_allocation_sha256", "preparation_contract_sha256", "corpus_sha256"):
            candidate = copy.deepcopy(loaded)
            candidate["typed_delta"][name] = "0" * 64
            with self.subTest(delta=name), self.assertRaises(ValueError):
                workload.measurement_binding(manifest, candidate)

    def test_measurement_receipt_refuses_other_phase_open_liability_and_unsealed_counts(self):
        manifest = {"backend": "elephant-db", "provider_allocation_sha256": "7" * 64}
        begin = {"format": "recall-exclusive-cohort-begin/v1", "authority": "exclusive_gateway_cohort",
                 "phase": "measurement", "backend": "elephant-db", "allocation_sha256": "7" * 64,
                 "cohort_id": 2, "baseline_sha256": "8" * 64}
        closed = {"format": "recall-exclusive-cohort-accounting/v1", "verdict": "passed", "cohort_id": 2,
                  "before_sha256": "8" * 64, "after_sha256": "9" * 64, "capture_inventory_sha256": "a" * 64,
                  "sealed_receipt_sha256": "b" * 64, "admission_closed": True, "unsettled_dispatches": 0,
                  "per_request_association": "unmet", "physical_dispatch_reservations": 100, "reserved_nano_usd": 106496000}
        workload.measurement_begin(manifest, begin)
        workload.measurement_close(begin, closed)
        for key, bad in (("phase", "preparation"), ("backend", "surreal-db"), ("allocation_sha256", "0" * 64),
                         ("baseline_sha256", ""), ("cohort_id", True), ("authority", "mock")):
            with self.subTest(begin=key), self.assertRaises(ValueError):
                workload.measurement_begin(manifest, {**begin, key: bad})
        for key, bad in (("cohort_id", 4), ("cohort_id", True), ("before_sha256", "0" * 64), ("admission_closed", False),
                         ("unsettled_dispatches", 1), ("unsettled_dispatches", False), ("physical_dispatch_reservations", 601),
                         ("reserved_nano_usd", 3000000001), ("reserved_nano_usd", 1),
                         ("sealed_receipt_sha256", ""), ("per_request_association", "inferred")):
            with self.subTest(close=key), self.assertRaises(ValueError):
                workload.measurement_close(begin, {**closed, key: bad})

    def test_accounting_loads_only_separate_pinned_g_source(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(workload, "ROOT", Path(directory)):
            path = Path(directory) / "tests/recall_preview_semantics/gateway/mock_bridge.py"
            path.parent.mkdir(parents=True)
            path.write_text("# Inert loader control; no authority implementation.\n")
            digest = workload.helpers.file_digest(path)
            manifest = {"provider_bridge_path": str(path), "provider_bridge_sha256": digest}
            review = {"provider_bridge_sha256": digest}
            bridge = SimpleNamespace(authorize_provider_allocation=Mock(), provider_accounting=Mock())
            with patch.object(workload.helpers, "module", return_value=bridge) as load:
                self.assertIs(workload.accounting_bridge(manifest, review), bridge)
                load.assert_called_once_with("governed_workload_accounting", path)
                bridge.authorize_provider_allocation.assert_not_called()
                bridge.provider_accounting.assert_not_called()
            for change in ("missing", "unreviewed", "outside-custody", "missing-interface"):
                with self.subTest(change=change):
                    candidate, pinned = dict(manifest), dict(review)
                    module = bridge
                    if change == "missing":
                        candidate["provider_bridge_path"] = str(path.with_name("absent.py"))
                    elif change == "unreviewed":
                        pinned["provider_bridge_sha256"] = "0" * 64
                    elif change == "outside-custody":
                        candidate["provider_bridge_path"] = str(Path(directory) / "foreign.py")
                    else:
                        module = SimpleNamespace()
                    with patch.object(workload.helpers, "module", return_value=module), self.assertRaises(ValueError):
                        workload.accounting_bridge(candidate, pinned)

    def test_inventory_never_claims_execution_and_freezes_50_total(self):
        with patch.object(workload.asyncio, "open_connection", side_effect=AssertionError("no outbound")):
            result = workload.inventory()
        self.assertFalse(result["product_acceptance"])
        self.assertEqual(result["fixed"]["source_count"], 50)
        self.assertEqual(result["fixed"]["request_count"], 200)

    def test_schedule_validates_exact_total_sources_callers_ordinals_and_originals(self):
        contract, corpus = protocol_fixture()
        workload.validate_schedule(contract, corpus)
        for change in ("100-sources", "199-requests", "duplicate-request", "duplicate-ordinal", "foreign-space", "rewritten-query", "lexical", "smaller-limit", "too-many-callers", "larger-timeout"):
            with self.subTest(change=change):
                candidate, inputs = copy.deepcopy(contract), copy.deepcopy(corpus)
                if change == "100-sources":
                    inputs["sources"] += [{"space": contract["spaces"][0], "key": f"extra-{index}"} for index in range(50)]
                elif change == "199-requests":
                    inputs["schedule"].pop()
                elif change == "duplicate-request":
                    inputs["schedule"][1]["id"] = inputs["schedule"][0]["id"]
                elif change == "duplicate-ordinal":
                    inputs["schedule"][1]["ordinal"] = 0
                elif change == "foreign-space":
                    inputs["schedule"][0]["space"] = contract["spaces"][1]
                elif change == "rewritten-query":
                    inputs["queries"][0]["query"] = ""
                elif change == "lexical":
                    inputs["queries"][0]["ranking_mode"] = "lexical"
                elif change == "smaller-limit":
                    inputs["queries"][0]["wire_limit"] = 10
                elif change == "too-many-callers":
                    candidate["concurrent_callers"] = 51
                else:
                    candidate["request_timeout_ms"] = 9000
                with self.assertRaises(ValueError):
                    workload.validate_schedule(candidate, inputs)

    def test_endpoint_refuses_ambient_dns_remote_redirect_targets_and_credentials(self):
        self.assertEqual(workload.endpoint("http://127.0.0.1:31000"), ("127.0.0.1", 31000))
        self.assertEqual(workload.endpoint("http://[::1]:31000"), ("::1", 31000))
        for value in ("http://localhost:31000", "http://example.com:80", "http://127.0.0.1", "http://user:secret@127.0.0.1:31000",
                      "http://127.0.0.1:31000/search", "http://127.0.0.1:31000?redirect=1", "https://127.0.0.1:31000"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                workload.endpoint(value)


class WorkloadAsyncControls(unittest.IsolatedAsyncioTestCase):
    async def frame(self, data, *, eof=True, **options):
        reader = asyncio.StreamReader(limit=workload.HEADER_CAP)
        reader.feed_data(data)
        if eof:
            reader.feed_eof()
        observed = {"raw_body": b"", "framing_complete": False, "body": None, "status": None}
        error = None
        try:
            async with asyncio.timeout(0.02):
                await workload.read_frame(reader, observed, **options)
        except (ValueError, TimeoutError, asyncio.IncompleteReadError, asyncio.LimitOverrunError) as caught:
            error = caught
        return observed, error

    async def test_content_length_chunked_and_eof_framing_preserve_exact_json(self):
        for frame in (b'HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\n{"ok":true}',
                      b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nb\r\n{"ok":true}\r\n0\r\n\r\n',
                      b'HTTP/1.0 200 OK\r\n\r\n{"ok":true}'):
            with self.subTest(frame=frame):
                value, error = await self.frame(frame)
                self.assertIsNone(error)
                self.assertTrue(value["framing_complete"])
                self.assertEqual(value["body"], {"ok": True})
                self.assertEqual(value["raw_body"], b'{"ok":true}')
                self.assertGreater(value["first_response_byte_ns"], 0)

    async def test_malformed_frames_fail_without_completed_body(self):
        for frame in (b'HTTP/1.1 200 OK\r\nContent-Length: 11\r\nContent-Length: 11\r\n\r\n{"ok":true}',
                      b'HTTP/1.1 200 OK\r\nContent-Length: 11\r\nTransfer-Encoding: chunked\r\n\r\n',
                      b'HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n\r\n',
                      b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nb\r\n{"ok":true}\r\n',
                      b'HTTP/1.1 200 OK\r\nContent-Length: 12\r\n\r\n{"ok":true}',
                      b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nContent-Length: 11\r\n\r\n',
                      b'HTTP/1.1 200 OK\r\nContent-Length: 262145\r\n\r\n',
                      b'HTTP/1.1 100 Continue\r\n\r\n'):
            with self.subTest(frame=frame):
                value, error = await self.frame(frame)
                self.assertIsNotNone(error)
                self.assertFalse(value["framing_complete"])

    async def test_explicit_mcp_envelope_bound_keeps_default_workload_limit(self):
        for framing in ("length", "chunked", "eof"):
            for size in (workload.MAX_TRANSPORT_BODY, workload.MAX_TRANSPORT_BODY + 1):
                body = b'"' + b'x' * (size - 2) + b'"'
                if framing == "length":
                    wire = b'HTTP/1.1 200 OK\r\nContent-Length: ' + str(size).encode() + b'\r\n\r\n' + body
                elif framing == "chunked":
                    wire = b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n' + format(size, 'x').encode() + b'\r\n' + body + b'\r\n0\r\n\r\n'
                else:
                    wire = b'HTTP/1.0 200 OK\r\n\r\n' + body
                with self.subTest(framing=framing, size=size):
                    observed, error = await self.frame(wire, body_limit=workload.MAX_TRANSPORT_BODY)
                    if size == workload.MAX_TRANSPORT_BODY:
                        self.assertIsNone(error)
                        self.assertTrue(observed["framing_complete"])
                        self.assertEqual(observed["raw_body"], body)
                    else:
                        self.assertIsInstance(error, ValueError)
                        self.assertFalse(observed["framing_complete"])
                    default_observed, default_error = await self.frame(wire)
                    self.assertIsInstance(default_error, ValueError)
                    self.assertFalse(default_observed["framing_complete"])
        self.assertEqual(workload.CAP, 262144)
        self.assertEqual(workload.FIXED["response_budget_bytes"], 262144)

    async def test_body_limit_must_be_explicit_bounded_integer_before_reads(self):
        for limit in (True, False, 0, -1, 270337, 262144.0, "262144", None):
            reader = AsyncMock()
            with self.subTest(limit=limit), self.assertRaisesRegex(ValueError, "transport body limit"):
                await workload.read_frame(reader, {}, body_limit=limit)
            reader.read.assert_not_awaited()
            reader.readexactly.assert_not_awaited()
            reader.readuntil.assert_not_awaited()

    async def test_absolute_deadline_keeps_partial_body_bytes(self):
        value, error = await self.frame(b'HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\n{"ok', eof=False)
        self.assertIsInstance(error, TimeoutError)
        self.assertFalse(value["framing_complete"])
        self.assertEqual(value["raw_body"], b'{"ok')

    async def test_framed_invalid_or_nonfinite_json_is_not_correctness(self):
        for body in (b'{"x":1e999}', b'{"x":1,"x":2}', b'not json'):
            observed, error = await self.frame(b'HTTP/1.1 200 OK\r\nContent-Length: ' + str(len(body)).encode() + b'\r\n\r\n' + body)
            self.assertIsNotNone(error)
            self.assertTrue(observed["framing_complete"])
            self.assertIsNone(observed["body"])
            self.assertEqual(observed["raw_body"], body)

    def stub(self, root):
        contract, corpus = protocol_fixture()
        value = workload.Workload.__new__(workload.Workload)
        value.manifest = {"expires_at": "2999-01-01T00:00:00Z", "space_credentials": {space: "service" for space in contract["spaces"]},
                          "source_commit": "1" * 40, "artifact_sha256": "2" * 64, "binary_sha256": "3" * 64,
                          "backend": "elephant-db", "endpoint": "http://127.0.0.1:31000",
                          "contract_sha256": "4" * 64, "corpus_sha256": "5" * 64, "prepared_fixture_sha256": "6" * 64,
                          "provider_phase": "measurement", "nonce": "synthetic-measurement-0001", "provider_allocation_sha256": "7" * 64,
                          "preparation_contract_sha256": "8" * 64, "typed_delta_sha256": "9" * 64, "rendered_map_sha256": "a" * 64}
        value.loaded = {"contract": contract, "corpus": corpus, "provider_allocation": {"mock_only": True}}
        value.queries = {row["id"]: row for row in corpus["queries"]}
        value.tokens = {"service": "private-synthetic-bearer"}
        value.host, value.port = "127.0.0.1", 31000
        value.root = root
        value.rows = []
        value.active_callers = value.peak_callers = value.capture_bytes = 0
        value.started_ns = time.monotonic_ns()
        value.adapter = Mock()
        value.adapter.workload_response.return_value = {"verdict": "failed", "reason": "no product oracle in this unit fixture"}
        value.adapter.request.side_effect = lambda query: {"query": query["query"], "limit": 50, "ranking_mode": "semantic"}
        value.adapter.evaluate_cohort.return_value = {"verdict": "failed", "reason": "mock incomplete cohort"}
        value.bridge = SimpleNamespace(authorize_provider_allocation=Mock(return_value={
                                           "format": "recall-exclusive-cohort-begin/v1", "authority": "exclusive_gateway_cohort",
                                           "phase": "measurement", "backend": "elephant-db", "allocation_sha256": "7" * 64,
                                           "baseline_sha256": "a" * 64, "cohort_id": 2}),
                                       provider_accounting=Mock(return_value={"verdict": "unmet"}))
        value.provider_cohort_begin = None
        value.callers = {}
        value.barrier_released_ms = None
        return value

    async def test_begin_refusal_prevents_measurement_and_still_closes_accounting(self):
        with tempfile.TemporaryDirectory() as directory:
            value = self.stub(Path(directory))
            value.started_ns = 0
            value.bridge.authorize_provider_allocation.side_effect = ValueError("original allocation refused")
            value.measure = AsyncMock()
            with patch.object(workload.asyncio, "open_connection", side_effect=AssertionError("no outbound")):
                result = await value.run()
            self.assertEqual(result, 1)
            value.measure.assert_not_awaited()
            value.bridge.provider_accounting.assert_called_once()
            report = json.loads((value.root / "observation.json").read_text())
            self.assertFalse(report["measured"])
            self.assertEqual(report["collector_verdict"], "failed")
            self.assertEqual(report["requests"], [])

    async def test_measurement_cancellation_finishes_original_accounting(self):
        with tempfile.TemporaryDirectory() as directory:
            value = self.stub(Path(directory))
            value.measure = AsyncMock(side_effect=asyncio.CancelledError())
            with patch.object(workload.asyncio, "open_connection", side_effect=AssertionError("no outbound")):
                self.assertEqual(await value.run(), 1)
            value.bridge.authorize_provider_allocation.assert_called_once()
            value.bridge.provider_accounting.assert_called_once()
            report = json.loads((value.root / "observation.json").read_text())
            self.assertEqual(report["run_error_type"], "CancelledError")
            self.assertEqual(report["provider_cohort_begin"], value.bridge.authorize_provider_allocation.return_value)
            self.assertFalse(report["product_acceptance"])

    async def test_accounting_failure_preserves_independent_performance_verdict(self):
        value = self.stub(Path("/never-written"))
        value.adapter.evaluate_cohort.return_value = {"verdict": "passed", "mock_only": True}
        for failure in (ValueError("ledger unavailable"), None, {"verdict": "unknown"}):
            with self.subTest(failure=failure):
                value.bridge.provider_accounting = Mock(side_effect=failure) if isinstance(failure, Exception) else Mock(return_value=failure)
                report = value.report()
                self.assertEqual(report["independent_verdict"]["verdict"], "passed")
                self.assertEqual(report["provider_accounting"]["verdict"], "unmet")
                self.assertEqual(report["collector_verdict"], "failed")
                self.assertIn("unmet", report["provider_request_association"])

    async def test_synchronous_stalled_accounting_has_an_absolute_deadline(self):
        value = self.stub(Path("/never-written"))
        value.adapter.evaluate_cohort.return_value = {"verdict": "passed", "mock_only": True}
        steps = []
        def stalled(*_args):
            steps.append("entered")
            time.sleep(1)
            steps.append("returned")
            return {"verdict": "passed"}
        value.bridge.provider_accounting.side_effect = stalled
        deadline = workload.helpers.capture.total_deadline
        with patch.object(workload.helpers.capture, "total_deadline", side_effect=lambda _seconds: deadline(0.01)) as timer:
            report = value.report()
        timer.assert_called_once_with(130)
        self.assertEqual(steps, ["entered"])
        self.assertEqual(report["provider_accounting"]["verdict"], "unmet")
        self.assertEqual(report["independent_verdict"]["verdict"], "passed")
        self.assertEqual(report["collector_verdict"], "failed")

    async def test_request_records_one_attempt_no_token_and_delegates_only_complete_body(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "captures").mkdir()
            value = self.stub(root)
            reader = asyncio.StreamReader()
            reader.feed_data(b'HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\n{"ok":true}')
            reader.feed_eof()
            writer = Mock()
            writer.drain = AsyncMock()
            writer.wait_closed = AsyncMock()
            with patch.object(workload.asyncio, "open_connection", AsyncMock(return_value=(reader, writer))) as open_connection:
                await value.request(value.loaded["corpus"]["schedule"][0])
            open_connection.assert_awaited_once()
            writer.write.assert_called_once()
            writer.close.assert_called_once()
            value.adapter.workload_response.assert_called_once()
            row = value.rows[0]
            self.assertEqual(row["oracle_verdict"], "failed")
            self.assertIsNone(row["provider_attempt_ids"])
            self.assertNotIn("private-synthetic-bearer", (root / "captures" / (row["id"] + ".json")).read_text())
            self.assertLess(row["started_ms"], row["ended_ms"])
            self.assertLessEqual(row["client_write_complete_ms"], row["first_response_byte_ms"])
            self.assertEqual(row["evaluation_fallbacks"], [])

    async def test_cancellation_is_captured_and_propagates_before_next_request(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "captures").mkdir()
            value = self.stub(root)
            started = asyncio.Event()

            async def blocked(*_args, **_kwargs):
                started.set()
                await asyncio.Event().wait()

            with patch.object(workload.asyncio, "open_connection", blocked):
                task = asyncio.create_task(value.request(value.loaded["corpus"]["schedule"][0]))
                await started.wait()
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            self.assertEqual(len(value.rows), 1)
            self.assertTrue(value.rows[0]["run_deadline_cancelled"])
            self.assertFalse(value.rows[0]["response"]["framing_complete"])
            self.assertEqual(value.active_callers, 0)

    async def test_cohort_has_exact_200_calls_at_most_50_and_order_per_caller(self):
        value = self.stub(Path("/never-written"))
        active = peak = 0
        calls = []

        async def request(row):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            calls.append((row["caller_id"], row["ordinal"]))
            await asyncio.sleep(0)
            active -= 1

        value.request = request
        with patch.object(workload.asyncio, "open_connection", side_effect=AssertionError("no socket")):
            await value.measure()
        self.assertEqual(len(calls), 200)
        self.assertLessEqual(peak, 50)
        self.assertEqual(active, 0)
        for worker in range(50):
            self.assertEqual([ordinal for caller, ordinal in calls if caller == f"caller-{worker:02}"], [0, 1, 2, 3])


if __name__ == "__main__":
    unittest.main()
