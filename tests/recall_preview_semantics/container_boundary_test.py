"""Mock-only driver checks: no product, subprocess, network, Docker or provider."""
import asyncio
import ast
import base64
import copy
import importlib.util
from pathlib import Path
import tempfile
import time
import types
import sys
import unittest
from unittest.mock import AsyncMock, Mock, patch

PATH = Path(__file__).resolve().parents[2] / "scripts/qualification/container_boundary_offline.py"
SPEC = importlib.util.spec_from_file_location("boundary_under_test", PATH)
driver = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(driver)


def observed(channel="source-a-rest", *, start=20, finish=40, status=503, body=None):
    return {"channel": channel, "started_ns": start, "finished_ns": finish,
            "deadline_ns": finish + 10, "transport_error": None, "framing_complete": True,
            "status": status, "body": body if body is not None else
                {"error_code": "not_ready", "message": "Recovering", "retryable": True},
            "capture": channel + ".json"}


def restore():
    return observed("restore", start=10, finish=100, status=200, body={"success": True})


def readiness(start=25, finish=35):
    return observed("readiness", start=start, finish=finish,
                    body={"ready": False, "recovery_ready": False})


class ClosedWorld(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for target, name in ((driver.base.subprocess, "Popen"), (asyncio, "open_connection"),
                             (asyncio, "start_server"), (asyncio, "start_unix_server")):
            deny = patch.object(target, name, side_effect=AssertionError("mock suite attempted process/network"))
            deny.start()
            self.addCleanup(deny.stop)


class ClassificationTests(ClosedWorld):
    def exercise_main_cancellation(self, mode, trigger):
        module = driver.base if mode == "offline" else driver
        path = self.root / (mode + "-" + trigger)
        allocation = {"run_directory": str(path), "expires_at": "2026-10-08T00:00:00+00:00"}
        now = [10.0]
        commands_created = []
        original_commands = driver.base.DockerCommands
        def commands_factory(value, directory, *, cancellation):
            result = original_commands(value, directory, monotonic=lambda: now[0], cancellation=cancellation)
            commands_created.append(result)
            return result
        original_handler = object()
        handlers, finished = [], []
        def install(_kind, handler):
            handlers.append(handler)
        def construct(*_args, commands):
            def execute():
                if trigger == "work":
                    handlers[0](driver.base.signal.SIGTERM, None)
                    commands.remaining()
                else:
                    now[0] += 1501
                    commands.remaining()
            def finish(failure):
                self.assertEqual(failure, "InterruptedError" if trigger == "work" else "ValueError")
                # First signal can arrive after a natural timeout has ALREADY
                # entered cleanup. Repeated signals cannot abort or renew it.
                self.assertTrue(commands.cancellation.cleaning)
                handlers[0](driver.base.signal.SIGTERM, None)
                self.assertEqual(commands.remaining(cleanup=True), 90)
                now[0] += 89
                handlers[0](driver.base.signal.SIGTERM, None)
                self.assertEqual(commands.remaining(cleanup=True), 1)
                now[0] += 1
                with self.assertRaisesRegex(ValueError, "deadline exceeded"):
                    commands.remaining(cleanup=True)
                finished.append(True)
                return {"product_acceptance": False}
            return types.SimpleNamespace(execute=execute, finish=finish)
        validated = (allocation, {}) if mode == "offline" else (allocation, {}, {})
        constructor = "ContainerDriver" if mode == "offline" else "ContainerBoundaryDriver"
        with patch.object(module, "validate_allocation", return_value=validated), \
             patch.object(module, constructor, side_effect=construct), \
             patch.object(driver.base, "DockerCommands", side_effect=commands_factory), \
             patch.object(driver.base.signal, "getsignal", return_value=original_handler), \
             patch.object(driver.base.signal, "signal", side_effect=install), \
             patch.object(driver.base.dt, "datetime", wraps=driver.base.dt.datetime) as date, \
             patch("builtins.print"):
            date.now.return_value = driver.base.utc("2026-10-07T00:00:00+00:00")
            self.assertEqual(module.main(["run", "--allocation", str(self.root / "unused.json")]), 1)
        self.assertEqual(finished, [True])
        self.assertEqual(handlers[-1], original_handler)
        self.assertEqual(len(commands_created), 1)

    def test_both_entrypoints_defer_work_signal_to_checkpoint_and_finish_owned_cleanup(self):
        for mode in ("offline", "boundary"):
            with self.subTest(mode=mode):
                self.exercise_main_cancellation(mode, "work")

    def test_both_entrypoints_ignore_first_and_repeated_signals_during_natural_timeout_cleanup(self):
        for mode in ("offline", "boundary"):
            with self.subTest(mode=mode):
                self.exercise_main_cancellation(mode, "natural")

    def test_plan_is_inert_and_covers_independent_ids(self):
        with patch.object(driver.base, "DockerCommands", side_effect=AssertionError("Docker initialized")):
            inventory = driver.plan()
        self.assertFalse(inventory["product_acceptance"])
        self.assertEqual(len(inventory["expectations"]["required_cases"]), 19)
        self.assertEqual(inventory["expectations"]["limits"]["combined_http_calls"], 1000)
        self.assertFalse(inventory["expectations"]["claims"]["continuous_barrier_proof"])

    def test_final_404_without_observed_barrier_is_unmet(self):
        row = observed(status=404, body={"error_code": "not_found", "message": "Absent", "retryable": False})
        result = driver.observed_restore_window(restore(), [row])
        self.assertEqual(result["verdict"], "unmet")
        self.assertFalse(result["continuous_barrier_proof"])

    def test_ready_and_refusal_must_overlap_each_other(self):
        result = driver.observed_restore_window(restore(), [readiness(20, 30), observed(start=40, finish=50)])
        self.assertEqual(result["verdict"], "unmet")

    def test_fully_inside_contemporaneous_refusal_is_sampled_pass_only(self):
        result = driver.observed_restore_window(restore(), [readiness(), observed()])
        self.assertEqual(result["verdict"], "pass")
        self.assertEqual(len(result["contemporaneous_refusals"]), 1)
        self.assertFalse(result["continuous_barrier_proof"])

    def test_requests_straddling_original_restore_are_not_barrier_evidence(self):
        for row in (observed(start=5), observed(finish=101)):
            self.assertEqual(driver.observed_restore_window(restore(), [readiness(), row])["verdict"], "unmet")

    def test_partial_frame_or_timeout_never_becomes_typed_success(self):
        for fields in ({"framing_complete": False}, {"transport_error": "TimeoutError"}):
            with self.assertRaisesRegex(ValueError, "incomplete"):
                driver.observed_restore_window(restore(), [readiness(), {**observed(), **fields}])

    def test_unavailable_is_not_recovery_barrier_refusal(self):
        row = observed(body={"error_code": "unavailable", "message": "Unavailable", "retryable": True})
        with self.assertRaisesRegex(ValueError, "category"):
            driver.observed_restore_window(restore(), [readiness(), row])

    def test_failed_original_restore_cannot_qualify_samples(self):
        for fields in ({"status": 500}, {"body": {"success": False}}):
            with self.assertRaises(ValueError):
                driver.observed_restore_window({**restore(), **fields}, [readiness(), observed()])

    def test_public_error_exact_shape_code_retry_and_message_budget(self):
        good = {"error_code": "not_found", "message": "Absent", "retryable": False}
        driver.public_error(good, "not_found", False)
        for delta in ({"internal_query": "SELECT private"}, {"retryable": True}, {"message": "x" * 513}, {"error_code": "unavailable"}):
            with self.assertRaises(ValueError):
                driver.public_error({**good, **delta}, "not_found", False)

    def test_transport_refusal_is_not_mcp_tool_result(self):
        with self.assertRaisesRegex(ValueError, "transport"):
            driver.mcp_result(observed(status=403))

    def test_mcp_error_requires_one_exact_typed_content(self):
        error = {"error_code": "not_found", "message": "Absent", "retryable": False}
        value = {"jsonrpc": "2.0", "id": 7, "result": {"isError": True,
            "content": [{"type": "text", "text": driver.base.encoded(error).decode()}]}}
        success, body = driver.mcp_result(observed(status=200, body=value))
        self.assertFalse(success)
        self.assertEqual(body, error)
        value["result"]["content"].append({"type": "image", "data": "unexpected"})
        with self.assertRaisesRegex(ValueError, "one complete"):
            driver.mcp_result(observed(status=200, body=value))

    def test_mcp_success_requires_success_data_envelope(self):
        value = {"result": {"content": [{"type": "text", "text": '{"ok":false}'}]}}
        with self.assertRaisesRegex(ValueError, "successful tool"):
            driver.mcp_result(observed(status=200, body=value))

    def test_complete_response_honors_absolute_deadline(self):
        row = observed()
        row["finished_ns"] = row["deadline_ns"] + 300_000_001
        with self.assertRaisesRegex(ValueError, "deadline"):
            driver.complete_response(row)


class ReviewTests(ClosedWorld):
    def fixture(self):
        boundary = driver.plan()
        boundary["dependency_sha256"] = {name: "a" * 64 for name in driver.BOUNDARY_DEPENDENCIES}
        review = {"format": "recall-container-boundary-review/v1", "reviewer": "qualification", "approved": True,
                  **{name: boundary[name] for name in ("driver_sha256", "expectations_sha256", "dependency_sha256")}}
        review_path = self.root / "review.json"
        review_path.write_bytes(driver.base.encoded(review))
        allocation = {"boundary_review": str(review_path), "boundary_review_sha256": driver.base.digest(review_path.read_bytes())}
        contract_path = self.root / "contract.json"
        contract_path.write_bytes(driver.base.encoded({"format": "recall-container-boundary-expectations/v1",
            "error_cases": [{"id": f"CB{i:02}"} for i in range(1, 11)],
            "coverage_cases": [{"id": f"CC{i:02}"} for i in range(1, 7)]}))
        return boundary, allocation, review_path, contract_path

    def validate(self, boundary, allocation, contract):
        with patch.object(driver.base, "validate_allocation", return_value=(allocation, {"base": "reviewed"})), \
             patch.object(driver, "plan", return_value=boundary), patch.object(driver, "CONTRACT_PATH", contract), \
             patch.object(driver, "framing_parser", side_effect=AssertionError("parser imported before review")):
            return driver.validate_allocation(self.root / "allocation.json")

    def test_extension_review_validates_without_importing_parser_or_starting_runtime(self):
        boundary, allocation, _review, contract = self.fixture()
        self.assertEqual(self.validate(boundary, allocation, contract)[2], boundary)

    def test_any_changed_extension_dependency_fails_before_parser_import(self):
        boundary, allocation, _review, contract = self.fixture()
        for name in driver.BOUNDARY_DEPENDENCIES:
            changed = copy.deepcopy(boundary)
            changed["dependency_sha256"][name] = "b" * 64
            with self.assertRaisesRegex(ValueError, "review changed"):
                self.validate(changed, allocation, contract)

    def test_review_bytes_and_missing_dependency_are_not_accepted(self):
        boundary, allocation, review, contract = self.fixture()
        review.write_bytes(review.read_bytes() + b" ")
        with self.assertRaisesRegex(ValueError, "digest changed"):
            self.validate(boundary, allocation, contract)
        boundary, allocation, review, contract = self.fixture()
        boundary["dependency_sha256"][driver.BOUNDARY_DEPENDENCIES[0]] = None
        body = driver.base.strict_json(review.read_bytes())
        body["dependency_sha256"] = boundary["dependency_sha256"]
        review.write_bytes(driver.base.encoded(body))
        allocation["boundary_review_sha256"] = driver.base.digest(review.read_bytes())
        with self.assertRaisesRegex(ValueError, "dependencies missing"):
            self.validate(boundary, allocation, contract)

    def test_contract_cannot_drop_an_independent_case(self):
        boundary, allocation, _review, contract = self.fixture()
        body = driver.base.strict_json(contract.read_bytes())
        body["coverage_cases"].pop()
        contract.write_bytes(driver.base.encoded(body))
        with self.assertRaisesRegex(ValueError, "contract changed"):
            self.validate(boundary, allocation, contract)


class Writer:
    def __init__(self):
        self.wire = None
        self.closed = False

    def write(self, value):
        self.wire = value

    async def drain(self):
        await asyncio.sleep(0)

    def close(self):
        self.closed = True

    async def wait_closed(self):
        return None


class TransportTests(ClosedWorld):
    def clients(self, parser=None, *, reserve=None, connect=None):
        self.writers = []
        async def connected(host, port, **kwargs):
            self.assertEqual((host, port), ("127.0.0.1", 34567))
            writer = Writer()
            self.writers.append(writer)
            return object(), writer
        async def parsed(_reader, value):
            await asyncio.sleep(0)
            value.update(status=200, raw_body=b'{"ready":true}', body={"ready": True}, framing_complete=True)
        return driver.IndependentClients(34567, self.root, {"service": "synthetic-secret"}, parser or parsed,
            lambda _raw, identity: {"jsonrpc": "2.0", "id": identity, "result": {}},
            time.monotonic_ns() + 10_000_000_000, connect=connect or connected, reserve=reserve)

    def test_real_client_writes_complete_private_capture_and_closes_connection(self):
        reserve = Mock()
        clients = self.clients(reserve=reserve)
        row = asyncio.run(clients.request("fixture", "GET", "/v1/readiness", "service", "household"))
        self.assertEqual(driver.complete_response(row), {"ready": True})
        self.assertEqual(reserve.call_count, 1)
        self.assertIn(b"Authorization: Bearer synthetic-secret", self.writers[0].wire)
        self.assertTrue(self.writers[0].closed)
        output = self.root / row["capture"]
        self.assertEqual(output.stat().st_mode & 0o777, 0o600)
        recorded = driver.base.strict_json(output.read_bytes())
        self.assertNotIn("synthetic-secret", output.read_text())
        self.assertEqual(base64.b64decode(recorded["raw_body_base64"]), b'{"ready":true}')

    def test_concurrent_clients_have_independent_connections_unique_ids_and_budget(self):
        clients = self.clients()
        async def run():
            return await asyncio.gather(*(clients.request(f"client-{i}", "GET", "/v1/readiness") for i in range(3)))
        rows = asyncio.run(run())
        self.assertEqual(len(self.writers), 3)
        self.assertEqual(len({row["capture"] for row in rows}), 3)
        self.assertEqual(clients.peak_active, 3)
        self.assertEqual(clients.active, 0)

    def test_absolute_deadline_covers_body_read_and_retains_partial_bytes(self):
        async def stalled(_reader, value):
            value.update(status=200, raw_body=b'{"ready":')
            await asyncio.sleep(1)
        clients = self.clients(stalled)
        with patch.object(driver, "REQUEST_SECONDS", 0.01):
            row = asyncio.run(clients.request("slow-body", "GET", "/v1/readiness"))
        self.assertEqual(row["transport_error"], "TimeoutError")
        self.assertFalse(row["framing_complete"])
        self.assertEqual(base64.b64decode(row["raw_body_base64"]), b'{"ready":')
        self.assertTrue(self.writers[0].closed)

    def test_absolute_deadline_covers_connection(self):
        async def stalled(*_args, **_kwargs):
            await asyncio.sleep(1)
        clients = self.clients(connect=stalled)
        with patch.object(driver, "REQUEST_SECONDS", 0.01):
            row = asyncio.run(clients.request("slow-connect", "GET", "/v1/readiness"))
        self.assertEqual(row["transport_error"], "TimeoutError")
        self.assertNotIn("connected_ns", row)

    def test_mcp_sse_fallback_requires_complete_frame_and_exact_content_type(self):
        async def partial(_reader, value):
            value.update(status=200, raw_body=b'data: {}', response_headers={"content-type": ["text/event-stream"]})
            raise ValueError("truncated")
        clients = self.clients(partial)
        row = asyncio.run(clients.request("sse", "POST", "/mcp/full", request_id=7))
        self.assertEqual(row["transport_error"], "ValueError")
        self.assertFalse(row["framing_complete"])

    def test_complete_sse_uses_original_request_id(self):
        async def sse(_reader, value):
            value.update(status=200, raw_body=b'data: {}', framing_complete=True,
                         response_headers={"content-type": ["text/event-stream; charset=utf-8"]})
            raise ValueError("not JSON")
        clients = self.clients(sse)
        row = asyncio.run(clients.request("sse", "POST", "/mcp/full", request_id=73))
        self.assertEqual(driver.complete_response(row)["id"], 73)

    def test_request_bound_header_injection_and_remote_path_fail_before_connect(self):
        clients = self.clients()
        for kwargs in ({"path": "http://example.invalid/v1/readiness"}, {"space": "household\r\nX:bad"},
                       {"body": {"x": "x" * driver.MAX_REQUEST_BYTES}}):
            args = {"channel": "bad", "method": "GET", "path": "/v1/readiness", **kwargs}
            with self.assertRaises(ValueError):
                asyncio.run(clients.request(**args))
        self.assertEqual(len(self.writers), 0)

    def test_allocation_and_capture_budgets_do_not_silently_drop_responses(self):
        clients = self.clients(reserve=Mock(side_effect=ValueError("combined exhausted")))
        with self.assertRaisesRegex(ValueError, "combined"):
            asyncio.run(clients.request("budget", "GET", "/v1/readiness"))
        self.assertEqual(len(self.writers), 0)
        clients = self.clients()
        with patch.object(driver, "MAX_CAPTURE_BYTES", 1), self.assertRaisesRegex(ValueError, "capture byte"):
            asyncio.run(clients.request("budget", "GET", "/v1/readiness"))
        self.assertTrue(self.writers[0].closed)


def inspection_report():
    return {"format": "recall-snapshot-inspection-v1", "manifest": {"sha256": "f" * 64},
        "row_count": 1, "statement_count": 1, "table_counts": {"source": 1},
        "rows": [{"identity": {"table": "source", "id": {"kind": "uuid", "value": "original"}},
                  "sha256": "a" * 64, "references": [], "record": {"kind": "map", "value": {}}}],
        "schema_statements": [{"sha256": "b" * 64, "statement": "DEFINE TABLE source SCHEMAFULL;"}]}


class InspectionTests(ClosedWorld):
    def test_active_custody_uses_the_same_pure_helper_without_runtime_imports(self):
        self.assertIs(driver.checked_inspection, driver.inspection_capture.checked_inspection)
        source = Path(driver.inspection_capture.__file__).read_bytes()
        tree = ast.parse(source)
        imports = {name.name for node in ast.walk(tree) if isinstance(node, ast.Import) for name in node.names}
        self.assertEqual(imports, {'json', 're'})
        self.assertEqual({node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}, {'__future__'})
        self.assertIn('scripts/qualification/snapshot_inspection_capture.py', driver.BOUNDARY_DEPENDENCIES)
        self.assertEqual(driver.inspection_capture.INSPECTION_CAP, 33554433)

    def test_pure_custody_rejects_duplicate_and_nonfinite_original_json(self):
        for raw in (b'{"format":"x","format":"y"}', b'{"format":NaN}', b'{"format":Infinity}'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                driver.inspection_capture.checked_inspection(raw, {})

    def test_complete_inventory_preserves_original_report_and_manifest(self):
        report = inspection_report()
        raw = driver.base.encoded(report)
        self.assertEqual(driver.checked_inspection(raw, report["manifest"]), report)
        with self.assertRaisesRegex(ValueError, "different registered"):
            driver.checked_inspection(raw, {"sha256": "e" * 64})

    def test_omitted_rows_duplicate_identity_or_inconsistent_counts_refuse(self):
        for change in (lambda value: value["rows"].clear(),
                       lambda value: value["rows"].append(copy.deepcopy(value["rows"][0])),
                       lambda value: value["table_counts"].update(source=2),
                       lambda value: value["rows"][0].pop("record"),
                       lambda value: value["schema_statements"][0].pop("statement")):
            report = inspection_report()
            change(report)
            with self.assertRaises(ValueError):
                driver.checked_inspection(driver.base.encoded(report), report["manifest"])

    def test_duplicate_rows_fail_even_when_counts_are_adjusted(self):
        report = inspection_report()
        report["rows"] *= 2
        report["row_count"] = 2
        report["table_counts"]["source"] = 2
        with self.assertRaisesRegex(ValueError, "duplicate physical"):
            driver.checked_inspection(driver.base.encoded(report), report["manifest"])

    def test_compact_custody_keeps_original_dictionary_and_references_unexpanded(self):
        report = inspection_report()
        report["format"] = "recall-snapshot-inspection-v2"
        original = b"\x01" * 2048
        key = driver.base.digest(original)
        report["blobs"] = {key: {"bytes": len(original),
            "base64": base64.b64encode(original).decode().rstrip("=")}}
        report["rows"][0]["record"]["value"]["text"] = {"kind": "string_ref", "value": key}
        raw = driver.base.encoded(report)
        checked = driver.checked_inspection(raw, report["manifest"], expected_format=report["format"])
        self.assertEqual(checked, report)
        self.assertEqual(checked["rows"][0]["record"]["value"]["text"]["kind"], "string_ref")
        self.assertNotIn("product_acceptance", checked)
        with self.assertRaisesRegex(ValueError, "unsupported"):
            driver.checked_inspection(raw, report["manifest"])

    def test_compact_requires_exact_requested_version_and_bounded_dictionary_shape(self):
        report = inspection_report()
        with self.assertRaisesRegex(ValueError, "unsupported"):
            driver.checked_inspection(driver.base.encoded(report), report["manifest"],
                                      expected_format="recall-snapshot-inspection-v2")
        for change in (lambda r: None,
                       lambda r: r.update(blobs=[]),
                       lambda r: r.update(blobs={"not-a-sha": {"bytes": 1, "base64": "AQ"}}),
                       lambda r: r.update(blobs={"a" * 64: {"bytes": True, "base64": "AQ"}}),
                       lambda r: r.update(blobs={"a" * 64: {"bytes": 14065537, "base64": "AQ"}}),
                       lambda r: r.update(blobs={"a" * 64: {"bytes": 1, "base64": "AQ", "extra": 1}}),
                       lambda r: r.update(blobs={f"{i:064x}": {"bytes": 14065536, "base64": "AQ"}
                                                for i in range(5)})):
            report = inspection_report()
            report["format"] = "recall-snapshot-inspection-v2"
            change(report)
            with self.assertRaises(ValueError):
                driver.checked_inspection(driver.base.encoded(report), report["manifest"],
                                          expected_format=report["format"])

    def test_actual_inspection_method_mounts_only_owned_readonly_backup_offline(self):
        value = object.__new__(driver.ContainerBoundaryDriver)
        value.root = self.root
        value.volumes = {"backups": "owned-backup-id", "auth": "must-not-mount", "recovery": "must-not-mount"}
        value.inspection_reports = {}
        value.metadata_canaries = {}
        value.erasure_acceptance = None
        value.physical_fixture = None
        value.physical_export_comparison = {"verdict": "not_executed"}
        value.inspection_capture_bytes = 0
        value.evidence_capture_bytes = 0
        value.graph_capture_held = 0
        value.graph_budget = None
        value.once = Mock(return_value="owned-inspector-id")
        report = inspection_report()
        report.update(format="recall-snapshot-inspection-v2", blobs={})
        raw = driver.base.encoded(report)
        value.copy = Mock(return_value=raw)
        backup = {"path": "/var/lib/meerkat-recall/backups/baseline", "manifest": report["manifest"]}
        evidence = value.inspect_backup(backup, "baseline")
        args, kwargs = value.once.call_args
        self.assertEqual(kwargs["mounts"], [("owned-backup-id", driver.base.RECALL_ROOT + "/backups", True)])
        self.assertNotIn("network", kwargs)
        self.assertNotIn("environment", kwargs)
        self.assertEqual(args[1][-2:], [backup["path"], "/tmp/recall-snapshot-inspection.json"])
        self.assertIn('--include-records', args[1][1])
        self.assertIn('--compact-records', args[1][1])
        self.assertEqual((self.root / evidence["capture"]).read_bytes(), raw)
        self.assertEqual((self.root / evidence["capture"]).stat().st_mode & 0o777, 0o600)
        self.assertNotIn("product_acceptance", evidence)

    def test_inspection_and_http_reports_share_capture_reservation(self):
        value = object.__new__(driver.ContainerBoundaryDriver)
        value.evidence_capture_bytes = driver.EVIDENCE_CAP - 10
        value.graph_capture_held = 0
        value.graph_budget = None
        value.reserve_capture(10)
        with self.assertRaisesRegex(ValueError, "combined private"):
            value.reserve_capture(1)

    def test_original_capture_bytes_reach_independent_decoder_without_reencoding(self):
        value = object.__new__(driver.ContainerBoundaryDriver)
        value.root = self.root
        raw = b'{ "original" : true }\n'
        path = self.root / "original-inspection.json"
        driver.base.private_new(path, raw)
        value.inspection_reports = {"baseline": {"capture": path.name,
            "bytes": len(raw), "sha256": driver.base.digest(raw)}}
        self.assertEqual(value.read_inspection_bytes("baseline"), raw)
        path.write_bytes(raw.replace(b"true", b"null"))
        with self.assertRaisesRegex(ValueError, "digest changed"):
            value.read_inspection_bytes("baseline")

    def test_short_retention_keeps_original_deadline_without_inspector_startup(self):
        value = object.__new__(driver.ContainerBoundaryDriver)
        value.inspect_backup = Mock(return_value={"capture": "typed-original"})
        value.capture_physical_files = Mock()
        with patch.object(driver.base.ContainerDriver, "backup", return_value={"path": "original"}) as original:
            self.assertEqual(value.backup("short-retention", 5), {"path": "original"})
            original.assert_called_once_with("short-retention", 5)
            value.inspect_backup.assert_not_called()
        with patch.object(driver.base.ContainerDriver, "backup", return_value={"path": "baseline"}):
            self.assertEqual(value.backup("baseline", 1200)["inspection"], {"capture": "typed-original"})
            value.inspect_backup.assert_called_once_with({"path": "baseline", "inspection": {"capture": "typed-original"}}, "baseline")


class ActiveExtensionTests(ClosedWorld):
    def fixture(self):
        value = object.__new__(driver.ContainerBoundaryDriver)
        value.boundary_inventory = driver.plan()
        value.boundary_cases = {name: {"verdict": "not_executed"}
                               for name in value.boundary_inventory["expectations"]["required_cases"]}
        value.save = Mock()
        value.boundary_failure = None
        value.allocation = {"backend": "elephant-db", "run_id": "mock-only"}
        value.total_http_calls = 0
        value.evidence_capture_bytes = 0
        value.graph_capture_held = 0
        value.graph_budget = None
        value.inspection_capture_bytes = 0
        value.inspection_reports = {}
        value.clients = None
        value.cases = {"spaces": ["household", "business"], "source_key": "synthetic/original"}
        value.baseline = {"household": {"head": {"generation": "original"}}}
        value.capture_physical_files = Mock()
        return value

    def physical_fixture(self):
        value = self.fixture()
        value.root = self.root
        value.physical_files = {}
        value.physical_file_evidence = {}
        value.volumes = {key: "owned-" + key for key in ("data", "database", "backups", "recovery")}
        value.server, value.database = "original-server", "original-database"
        value.original_journal = {"journal-id": "original-journal"}
        interpreter = Mock(ROOTS=("data", "database", "backups", "registry"), CAPTURE_CAP=driver.EVIDENCE_CAP,
                           ORIGIN_CAP=64 * 1024 * 1024)
        interpreter.archive.return_value = {}
        value.physical_inventory_module = Mock(return_value=interpreter)
        value.events = []
        value.stop = Mock(side_effect=lambda container=None: value.events.append(("stop", container)))
        value.once = Mock(side_effect=lambda *args, **kwargs: value.events.append(("helper", kwargs)) or "readonly-helper")
        def copied(_container, path, maximum):
            value.events.append(("copy", path))
            if path.endswith(("history-presence.txt", "origin-presence.txt")):
                return b"absent"
            return b"original-journal" if path.endswith("journal-id") else b"non-secret-original"
        value.copy = Mock(side_effect=copied)
        def command(arguments, **_kwargs):
            value.events.append(("command", arguments))
            return 0, b"original-archive", b""
        value.commands = Mock()
        value.commands.run.side_effect = command
        value.wait_database = Mock(side_effect=lambda: value.events.append(("database-ready",)))
        value.wait_ready = Mock(side_effect=lambda: value.events.append(("service-ready",)))
        value.capture_private_membership = Mock(side_effect=lambda phase:
            value.events.append(("private-inspection", phase)) or (b"private-original", {"capture": "private.json"}))
        # Exercise the actual method; the ordinary fixture separately mocks this
        # new collaborator for its original export/intent-focused controls.
        del value.capture_physical_files
        return value

    def test_physical_capture_stops_both_writers_before_readonly_copies_and_restarts_in_order(self):
        value = self.physical_fixture()
        value.capture_physical_files("pre")
        self.assertEqual(value.events[:3], [("stop", None), ("private-inspection", "pre"), ("stop", "original-database")])
        _, helper = value.events[3]
        self.assertTrue(all(readonly for _, _, readonly in helper["mounts"]))
        self.assertNotIn("network", helper)
        self.assertNotIn("environment", helper)
        self.assertEqual({volume for volume, _, _ in helper["mounts"]}, set(value.volumes.values()))
        self.assertNotIn("reference-key.json", repr(value.events))
        self.assertIn("find /physical/data /physical/database /physical/backups -printf", value.once.call_args.args[1][1])
        self.assertIn(("copy", "/tmp/recall-physical-metadata.bin"), value.events)
        value.physical_inventory_module.return_value.file_metadata.assert_called_once_with(b"non-secret-original", {}, set())
        self.assertEqual(value.events[-4:], [("command", ["start", "original-database"]),
            ("database-ready",), ("command", ["start", "original-server"]), ("service-ready",)])
        self.assertTrue(value.physical_files["pre"]["stopped"])
        self.assertEqual(set(value.physical_files["pre"]["archives"]), {"data", "database", "backups", "registry"})
        self.assertEqual(value.physical_files["pre"]["metadata"], b"non-secret-original")
        self.assertEqual(value.physical_files["pre"]["private_membership"], b"private-original")
        self.assertGreater(value.evidence_capture_bytes, 0)
        with self.assertRaisesRegex(ValueError, "repeated"):
            value.capture_physical_files("pre")

    def test_physical_fixture_preserves_original_four_field_oracle_contract(self):
        value = self.fixture()
        expected = {"capture": "raw.json", "sha256": "a" * 64, "bytes": 77, "manifest": {"original": True}}
        value.inspection_reports["baseline"] = {**expected, "backup_manifest_sha256": "b" * 64,
            "row_count": 2, "table_counts": {"source": 2}, "scope": "collection metadata"}
        self.assertEqual(value.physical_snapshot_reference("baseline"), expected)
        self.assertEqual(value.inspection_reports["baseline"]["row_count"], 2)

    def test_native_private_transport_uses_only_original_network_and_fixed_readonly_query(self):
        value = self.physical_fixture()
        del value.capture_private_membership
        value.network = "original-owned-network"
        value.environment = {"SURREALDB_USER": "qualification", "SURREALDB_PASS": "synthetic-only",
                             "UNRELATED_TOKEN": "never-forward"}
        value.private_membership_module = Mock(return_value=Mock(
            QUERY="INFO FOR DB; SELECT * FROM source_erasure_membership;", RAW_CAP=32 * 1024 * 1024))
        value.commands.remaining.return_value = 25
        value.copy.side_effect = lambda container, path, maximum: b"200" if path.endswith("status.txt") else b"complete-private-response"
        payload, evidence = value.capture_private_membership("post")
        self.assertEqual(payload, b"complete-private-response")
        self.assertEqual(evidence["http_status"], 200)
        args, kwargs = value.once.call_args
        self.assertEqual(kwargs["network"], value.network)
        self.assertEqual(kwargs["environment"], {key: value.environment[key] for key in ("SURREALDB_USER", "SURREALDB_PASS")})
        self.assertNotIn("mounts", kwargs)
        self.assertNotIn("user", kwargs)
        self.assertIn('--noproxy "*"', args[1][1])
        self.assertIn('http://paired-store:8000/sql', args[1][1])
        self.assertNotIn("--location", args[1][1])
        self.assertEqual(args[1][3], value.private_membership_module.return_value.QUERY)
        self.assertEqual(args[1][4], "25")
        self.assertEqual(args[1][5], str(32 * 1024 * 1024))
        self.assertTrue(evidence["writer_stopped"])

    def test_private_inspection_failure_cannot_publish_stopped_volume_success(self):
        value = self.physical_fixture()
        value.capture_private_membership.side_effect = ValueError("private read incomplete")
        with self.assertRaisesRegex(ValueError, "private read incomplete"):
            value.capture_physical_files("post")
        self.assertEqual(value.events, [("stop", None)])
        self.assertEqual(value.physical_files, {})
        value.once.assert_not_called()
        value.wait_ready.assert_not_called()

    def test_present_history_uses_only_exact_nonsecret_directory_and_original_budget(self):
        value = self.physical_fixture()
        original_copy = value.copy.side_effect
        value.copy.side_effect = lambda container, path, maximum: (b"present" if path.endswith("history-presence.txt")
                                                                  else original_copy(container, path, maximum))
        value.capture_physical_files("post")
        self.assertEqual(value.physical_files["post"]["membership"]["history"], b"original-archive")
        self.assertEqual(value.physical_file_evidence["post"]["archives"]["history_presence"], "present")
        history_calls = [call for call in value.commands.run.call_args_list
                         if "/physical/journal/history-commitments" in repr(call.args)]
        self.assertEqual(len(history_calls), 1)
        self.assertTrue(history_calls[0].kwargs["secret"])
        self.assertLess(history_calls[0].kwargs["output_limit"], driver.EVIDENCE_CAP)
        self.assertIn("test ! -L /physical/journal/history-commitments", value.once.call_args.args[1][1])
        self.assertNotIn("reference-key.json", repr(value.events))

    def test_incomplete_history_presence_prevents_publication_and_restart(self):
        value = self.physical_fixture()
        original_copy = value.copy.side_effect
        value.copy.side_effect = lambda container, path, maximum: (b"unknown" if path.endswith("history-presence.txt")
                                                                  else original_copy(container, path, maximum))
        with self.assertRaisesRegex(ValueError, "history presence capture incomplete"):
            value.capture_physical_files("post")
        self.assertEqual(value.physical_files, {})
        value.wait_database.assert_not_called()
        value.wait_ready.assert_not_called()

    def test_restore_origins_capture_uses_only_exact_nonsecret_file_and_original_budget(self):
        value = self.physical_fixture()
        original_copy = value.copy.side_effect
        value.copy.side_effect = lambda container, path, maximum: (b"present" if path.endswith("origin-presence.txt")
                                                                  else original_copy(container, path, maximum))
        value.capture_physical_files("replay")
        self.assertEqual(value.physical_files["replay"]["membership"]["origins"], b"non-secret-original")
        evidence = value.physical_file_evidence["replay"]["archives"]
        self.assertEqual(evidence["restore_origin_presence"], "present")
        self.assertFalse(evidence["restore_origins"]["mac_verified_by_collector"])
        copies = [call for call in value.copy.call_args_list if call.args[1].endswith("imported-history-origins.json")]
        self.assertEqual(len(copies), 1)
        self.assertEqual(copies[0].args[2], 64 * 1024 * 1024)
        self.assertIn("test ! -L /physical/journal/imported-history-origins.json", value.once.call_args.args[1][1])
        self.assertNotIn("reference-key.json", repr(value.events))

    def test_restore_origin_presence_failure_prevents_published_phase(self):
        value = self.physical_fixture()
        original_copy = value.copy.side_effect
        value.copy.side_effect = lambda container, path, maximum: (b"unknown" if path.endswith("origin-presence.txt")
                                                                  else original_copy(container, path, maximum))
        with self.assertRaisesRegex(ValueError, "restore origin presence capture incomplete"):
            value.capture_physical_files("replay")
        self.assertEqual(value.physical_files, {})
        value.wait_ready.assert_not_called()

    def test_physical_capture_failure_never_publishes_phase_success_or_restarts_work(self):
        value = self.physical_fixture()
        value.commands.run.side_effect = ValueError("bounded capture failed")
        with self.assertRaisesRegex(ValueError, "bounded capture failed"):
            value.capture_physical_files("post")
        self.assertEqual(value.physical_files, {})
        self.assertEqual(value.physical_file_evidence, {})
        value.wait_database.assert_not_called()
        value.wait_ready.assert_not_called()
        self.assertNotIn("start", repr(value.commands.run.call_args_list))

    def test_physical_archives_cannot_create_an_additional_evidence_allowance(self):
        value = self.physical_fixture()
        value.evidence_capture_bytes = driver.EVIDENCE_CAP - 100
        with self.assertRaisesRegex(ValueError, "remaining original"):
            value.capture_physical_files("pre")
        value.commands.run.assert_not_called()
        self.assertEqual(value.physical_files, {})

    def test_base_pass_never_turns_unmet_extension_into_acceptance(self):
        value = self.fixture()
        value.unmet("CB07", "Actual fixture absent")
        with patch.object(driver.base.ContainerDriver, "finish", return_value={"product_acceptance": True}):
            report = value.finish()
        self.assertFalse(report["product_acceptance"])
        self.assertEqual(report["cases"]["CB07"]["verdict"], "unmet")

    def test_failed_case_is_recorded_and_propagated(self):
        value = self.fixture()
        def fail():
            raise ValueError("hidden details")
        with self.assertRaises(ValueError):
            value.boundary_case("CB01", fail)
        self.assertEqual(value.boundary_cases["CB01"]["verdict"], "fail")
        self.assertNotIn("hidden details", str(value.boundary_cases))
        with self.assertRaisesRegex(ValueError, "repeated"):
            value.boundary_case("CB01", lambda: {})

    def test_disabled_case_restores_original_actual_runtime_mode_on_failure(self):
        value = self.fixture()
        value.tool = Mock(side_effect=[{"runtime_mode": "shared_memory"}, {"runtime_mode": "read_only"},
                                      {"runtime_mode": "shared_memory"}])
        async def failure(*_args, **_kwargs):
            raise ValueError("expected negative failed")
        value.error_pair = failure
        with self.assertRaises(ValueError):
            value.disabled_pair()
        self.assertEqual(value.tool.call_args.args, ("configure_runtime", {"runtime_mode": "shared_memory"}))

    def test_native_unavailable_restarts_only_original_owned_pair_after_failure(self):
        value = self.fixture()
        value.allocation["backend"] = "surreal-db"
        value.database = "owned-immutable-pair-id"
        value.http = Mock(return_value={"status": 200})
        value.stop = Mock()
        value.commands = Mock()
        value.wait_database = Mock()
        value.wait_ready = Mock()
        async def failure(*_args, **_kwargs):
            raise ValueError("timeout does not prove unavailable")
        value.error_pair = failure
        with self.assertRaises(ValueError):
            value.unavailable_pair()
        value.stop.assert_called_once_with("owned-immutable-pair-id")
        value.commands.run.assert_called_once_with(["start", "owned-immutable-pair-id"])
        value.wait_database.assert_called_once()

    def test_embedded_unavailable_without_supported_control_is_unmet(self):
        self.assertEqual(self.fixture().unavailable_pair()["verdict"], "unmet")

    def test_original_base_and_independent_requests_share_one_budget(self):
        value = self.fixture()
        with patch.object(driver.base.ContainerDriver, "http", return_value={"status": 200}):
            value.http("GET", "/v1/readiness")
        value.reserve_http()
        self.assertEqual(value.total_http_calls, 2)
        value.total_http_calls = driver.MAX_REQUESTS
        with self.assertRaisesRegex(ValueError, "combined"):
            value.reserve_http()

    def test_metadata_canary_is_distinct_and_stable_in_actual_request_without_document(self):
        value = self.fixture()
        value.metadata_canaries = {}
        body = {"name": "same-key", "source_type": "manual", "ingest_mode": "metadata",
                "intake": {"revision_id": "metadata-v1"}}
        with patch.object(driver.base.ContainerDriver, "http", return_value={"status": 202, "body": {}}) as actual:
            value.http("POST", "/v1/sources/ingest", "service", "household", body=body)
            first = actual.call_args.kwargs["body"]
            value.http("POST", "/v1/sources/ingest", "service", "business", body=body)
            second = actual.call_args.kwargs["body"]
            value.http("POST", "/v1/sources/ingest", "service", "household", body=body)
            self.assertEqual(first, actual.call_args.kwargs["body"])
        self.assertNotEqual(first["uri"], second["uri"])
        self.assertEqual(set(first), {*body, "uri"})
        self.assertNotIn("uri", body)

    def test_independent_export_pass_does_not_complete_physical_gate(self):
        value = self.fixture()
        value.physical_fixture = {"format": "recall-physical-fixture/v1"}
        value.read_inspection_bytes = Mock(side_effect=[b'{"pre": true}\n', b'{"post": true}\n', b'{"replay": true}\n'])
        evaluator = Mock()
        evaluator.evaluate_export_bytes.return_value = {"verdict": "pass", "scope": "exported_inventory", "product_acceptance": False}
        with patch.object(driver, "physical_oracle", return_value=evaluator):
            outcome = value.evaluate_physical_exports()
        self.assertEqual(outcome["verdict"], "unmet")
        evaluator.evaluate_export_bytes.assert_called_once_with(b'{"pre": true}\n', b'{"post": true}\n', b'{"replay": true}\n', value.physical_fixture)
        self.assertIn("PV/WAL/media", outcome["reason"])

    def test_independent_export_failure_propagates_without_reclassification(self):
        value = self.fixture()
        value.physical_fixture = {"format": "recall-physical-fixture/v1"}
        value.read_inspection_bytes = Mock(return_value=b"{}\n")
        evaluator = Mock()
        evaluator.evaluate_export_bytes.return_value = {"verdict": "fail", "scope": "exported_inventory", "product_acceptance": False}
        with patch.object(driver, "physical_oracle", return_value=evaluator):
            self.assertEqual(value.evaluate_physical_exports()["verdict"], "fail")

    def test_export_evaluator_cannot_expand_its_acceptance_scope(self):
        for change in ({"scope": "full_physical_gate"}, {"product_acceptance": True}, {"product_acceptance": None}):
            value = self.fixture()
            value.physical_fixture = {"format": "recall-physical-fixture/v1"}
            value.read_inspection_bytes = Mock(return_value=b"{}\n")
            evaluator = Mock()
            evaluator.evaluate_export_bytes.return_value = {"verdict": "pass", "scope": "exported_inventory",
                                                       "product_acceptance": False, **change}
            with patch.object(driver, "physical_oracle", return_value=evaluator):
                with self.assertRaisesRegex(ValueError, "invalid component"):
                    value.evaluate_physical_exports()

    def test_replay_capture_precedes_actual_restore_request(self):
        value = self.fixture()
        value.restore_phase = "replay"
        order = []
        value.capture_original_erasure = Mock(side_effect=lambda: order.append("capture"))
        async def restore_request(_backup):
            order.append("restore")
            return {"verdict": "unmet", "continuous_barrier_proof": False}
        value.observe_restore = restore_request
        value.wait_ready = Mock(return_value=[{"ready": True}])
        self.assertEqual(value.restore({"actual_original_backup": True}), [{"ready": True}])
        self.assertEqual(order, ["capture", "restore"])
        self.assertEqual(value.boundary_cases["CB10"]["verdict"], "unmet")

    def test_capture_uses_actual_completed_status_and_original_journal_intent(self):
        value = self.fixture()
        value.physical_fixture = None
        value.erasure_acceptance = {"operation_id": "original-erasure-op"}
        value.before_erasure_intents = {}
        now = driver.base.dt.datetime.now(driver.base.dt.timezone.utc)
        value.erasure_started_at = (now - driver.base.dt.timedelta(seconds=2)).isoformat()
        intent = {"intent_id": "original-journal-intent", "source_id": "source:A", "space_id": "household",
                  "recorded_at": (now - driver.base.dt.timedelta(seconds=1)).isoformat()}
        value.journal_intents = Mock(return_value={"original.json": {"intent": intent}})
        value.original_journal = {"journal-id": "original-journal", "reference-key-fingerprint": "f" * 64}
        value.inspection_reports = {"baseline": {"capture": "pre"}, "post-erasure-inspection": {"capture": "post"}}
        value.backup = Mock()
        completed = {"operation_id": "original-erasure-op", "state": "complete", "lifecycle": "erased", "cleanup_state": "complete"}
        value.operation = Mock(return_value=completed)
        value.accepted = {space: {"source_id": source, "operation": {"desired_revision": "metadata-v1"}}
                          for space, source in (("household", "source:A"), ("business", "source:B"))}
        value.baseline = {space: {"source": {"id": row["source_id"]}} for space, row in value.accepted.items()}
        value.metadata_canaries = {(space, "synthetic/original", "metadata-v1"): "unique:" + space
                                  for space in value.accepted}
        value.capture_original_erasure()
        fixture = value.physical_fixture
        self.assertEqual(fixture["erasure"]["completed"], completed)
        self.assertEqual(fixture["journal"]["original_erasure_intent"], intent)
        self.assertNotIn("erase_completed_at", fixture["journal"])
        self.assertGreaterEqual(driver.base.utc(fixture["journal"]["erase_completed_observed_at"]), driver.base.utc(intent["recorded_at"]))
        self.assertEqual(fixture["snapshots"], {"pre": {"capture": "pre"}, "post": {"capture": "post"}})
        value.backup.assert_called_once_with("post-erasure-inspection", 300)
        with self.assertRaisesRegex(ValueError, "repeated"):
            value.capture_original_erasure()


def journal_archive(rows, *, entry_type=None):
    output = driver.base.io.BytesIO()
    with driver.base.tarfile.open(fileobj=output, mode="w") as archive:
        for name, body in rows:
            member = driver.base.tarfile.TarInfo(name)
            member.size = len(body)
            if entry_type:
                member.type = entry_type
                member.linkname = "../../reference-key.json"
                member.size = 0
            archive.addfile(member, driver.base.io.BytesIO(body) if not entry_type else None)
    return output.getvalue()


class JournalCaptureTests(ClosedWorld):
    def original(self):
        identity = "150057e5-6b28-4ccc-a0f6-bdac268be4c6"
        intent = {"intent_id": identity, "space_id": "household",
                  "source_id": "source:faeb83cb-e2fa-4d02-bc77-892af32b7641", "recorded_at": "2026-10-07T01:00:00Z"}
        return "erasures/" + identity + ".json", driver.base.encoded(intent)

    def test_original_payload_free_bytes_and_digest_are_retained(self):
        name, body = self.original()
        rows = driver.checked_journal_tar(journal_archive([(name, body)]))
        self.assertEqual(rows[Path(name).name]["sha256"], driver.base.digest(body))
        self.assertEqual(base64.b64decode(rows[Path(name).name]["body_base64"]), body)
        self.assertEqual(driver.checked_journal_tar(journal_archive([])), {})

    def test_links_escapes_duplicates_and_unrelated_files_refuse(self):
        name, body = self.original()
        for raw in (journal_archive([(name, body)], entry_type=driver.base.tarfile.SYMTYPE),
                    journal_archive([(name, body)], entry_type=driver.base.tarfile.LNKTYPE),
                    journal_archive([("../" + name, body)]),
                    journal_archive([("/" + name, body)]),
                    journal_archive([(name, body), (name, body)]),
                    journal_archive([("reference-key.json", body)]),
                    journal_archive([("erasures/nested/" + Path(name).name, body)])):
            with self.assertRaises(ValueError):
                driver.checked_journal_tar(raw)

    def test_unknown_payload_and_filename_mismatch_are_not_control_authority(self):
        name, body = self.original()
        original = driver.base.strict_json(body)
        for delta in ({"source_text": "forbidden"}, {"intent_id": "foreign"}, {"source_id": "entity:faeb83cb-e2fa-4d02-bc77-892af32b7641"}):
            with self.assertRaises(ValueError):
                driver.checked_journal_tar(journal_archive([(name, driver.base.encoded({**original, **delta}))]))

    def test_actual_journal_collector_mounts_only_original_readonly_volume(self):
        value = object.__new__(driver.ContainerBoundaryDriver)
        value.volumes = {"recovery": "original-journal-volume", "auth": "never-mount", "data": "never-mount"}
        value.original_journal = {"journal-id": "90850b91-c06c-4a95-8a1d-5d9ae8c9b8a9"}
        value.once = Mock(return_value="owned-readonly-journal-container")
        value.copy = Mock(return_value=value.original_journal["journal-id"].encode())
        value.commands = Mock()
        value.commands.run.return_value = (0, journal_archive([self.original()]), b"")
        value.save = Mock()
        result = value.journal_intents("before-erasure")
        self.assertEqual(len(result), 1)
        self.assertEqual(value.once.call_args.kwargs["mounts"],
                         [("original-journal-volume", driver.base.RECALL_ROOT + "/recovery", True)])
        self.assertNotIn("network", value.once.call_args.kwargs)
        self.assertNotIn("environment", value.once.call_args.kwargs)
        value.copy.assert_called_once_with("owned-readonly-journal-container", driver.base.RECALL_ROOT + "/recovery/journal-id", 512)
        command = value.commands.run.call_args.args[0]
        self.assertTrue(command[2].endswith("/recovery/erasures"))
        self.assertNotIn("reference-key", str(command))


GRAPH_PATH = PATH.with_name("container_graph_runtime.py")
GRAPH_SPEC = importlib.util.spec_from_file_location("container_graph_under_test", GRAPH_PATH)
graph = importlib.util.module_from_spec(GRAPH_SPEC)
GRAPH_SPEC.loader.exec_module(graph)
BUDGET_SPEC = importlib.util.spec_from_file_location("container_budget_under_test", PATH.with_name("container_graph_budget.py"))
budget = importlib.util.module_from_spec(BUDGET_SPEC)
BUDGET_SPEC.loader.exec_module(budget)


class GraphAdapterTests(ClosedWorld):
    def ledger(self, *, calls=3, written=20, maximum_calls=5, maximum_bytes=100):
        return budget.BudgetLedger.create(self.root / "budget.json", "original-nonce", calls=calls,
            written=written, maximum_calls=maximum_calls, maximum_bytes=maximum_bytes,
            deadline_ns=time.monotonic_ns() + 30_000_000_000)

    def test_original_shared_budget_counts_separate_owners_and_never_resets(self):
        first = self.ledger()
        second = budget.BudgetLedger(first.path, "original-nonce")
        self.assertEqual(first.change(calls=1)["calls"], 4)
        self.assertEqual(second.change(calls=1)["calls"], 5)
        with self.assertRaisesRegex(ValueError, "exhausted"):
            first.change(calls=1)
        self.assertEqual(second.change()["calls"], 5)
        with self.assertRaises(FileExistsError):
            self.ledger()

    def test_provider_reservations_exclude_public_capture_until_released(self):
        first = self.ledger()
        second = budget.BudgetLedger(first.path, "original-nonce")
        first.change(held=(0, 70))
        with self.assertRaisesRegex(ValueError, "exhausted"):
            second.change(written=11)
        second.change(written=10)
        first.change(held=(70, 0), written=60)
        self.assertEqual(second.change()["written"], 90)
        with self.assertRaisesRegex(ValueError, "different original state"):
            first.change(held=(70, 0))

    def test_corrupt_foreign_symlink_and_expired_budget_do_not_reinitialize(self):
        first = self.ledger()
        with self.assertRaisesRegex(ValueError, "identity"):
            budget.BudgetLedger(first.path, "different-nonce").change()
        alias = self.root / "alias.json"
        alias.symlink_to(first.path)
        with self.assertRaisesRegex(ValueError, "canonical"):
            budget.BudgetLedger(alias, "original-nonce")
        raw = driver.base.strict_json(first.path.read_bytes())
        raw["deadline_ns"] = time.monotonic_ns() - 1
        first.path.write_bytes(driver.base.encoded(raw))
        with self.assertRaisesRegex(ValueError, "deadline"):
            first.change(calls=1)
        self.assertEqual(first.change(allow_expired=True)["calls"], 3)
        first.path.write_bytes(b'{"incomplete":')
        with self.assertRaises(ValueError):
            first.change()
        self.assertEqual(first.path.read_bytes(), b'{"incomplete":')

    def test_driver_reserves_from_same_original_ledger(self):
        value = object.__new__(driver.ContainerBoundaryDriver)
        value.graph_budget = self.ledger()
        value.total_http_calls, value.evidence_capture_bytes = 3, 20
        value.reserve_http()
        value.graph_budget.change(calls=1, held=(0, 70))
        with self.assertRaisesRegex(ValueError, "exhausted"):
            value.reserve_http()
        with self.assertRaisesRegex(ValueError, "exhausted"):
            value.reserve_capture(11)
        value.reserve_capture(10)
        self.assertEqual(value.evidence_capture_bytes, 30)

    def test_graph_client_keeps_original_role_deadline_transport_and_failure(self):
        clients = Mock(deadline_ns=40_000_000_000, clock_ns=Mock(return_value=10_000_000_000))
        clients.request = AsyncMock(return_value={"transport_error": "TimeoutError", "framing_complete": False,
                                                  "status": None, "body": None})
        client = graph.GraphClient(clients, "original-space")
        observed = asyncio.run(client.request("GET", "/v1/docs/original", "original-space", role="data"))
        self.assertEqual(client.deadline, 40)
        self.assertEqual(client.clock(), 10)
        self.assertEqual(observed["error_type"], "TimeoutError")
        clients.request.assert_awaited_once_with("graph-public", "GET", "/v1/docs/original",
                                               "graph-service", "original-space", None)
        for space, role in (("different", "data"), ("original-space", "manage")):
            with self.assertRaises(ValueError):
                asyncio.run(client.request("GET", "/v1/docs/original", space, role=role))
        self.assertEqual(clients.request.await_count, 1)

    def test_graph_operation_polls_keep_identity_and_share_original_deadline(self):
        now = [10_000_000_000]
        clients = Mock(deadline_ns=40_000_000_000, clock_ns=lambda: now[0])
        clients.request = AsyncMock(return_value={"transport_error": None})
        client = graph.GraphClient(clients, "original-space")
        async def sleep(duration):
            now[0] += int(duration * 1e9)
        async def calls():
            with patch.object(graph.asyncio, "sleep", side_effect=sleep) as sleeper:
                await client.request("GET", "/v1/operations/original", "original-space", role="data")
                await client.request("GET", "/v1/operations/original", "original-space", role="data")
                sleeper.assert_awaited_once_with(0.5)
        asyncio.run(calls())
        self.assertEqual(clients.request.await_count, 2)
        self.assertEqual(client.deadline, 40)

    def test_endpoint_materialization_changes_only_original_placeholder(self):
        fixture = Mock(require=driver.require)
        original = {"name": "default", "processing": {"connections": {"controlled": {
            "endpoint": "{owned_spy_endpoint}", "credential_ref": "RECALL_BOUNDARY_SPY_KEY", "revoked": False}}}}
        endpoint = "http://127.0.0.1:43001/v1"
        changed = graph.processing_for_endpoint(fixture, original, endpoint)
        self.assertEqual(changed["processing"]["connections"]["controlled"]["endpoint"], endpoint)
        self.assertEqual(original["processing"]["connections"]["controlled"]["endpoint"], "{owned_spy_endpoint}")
        for field, value in (("endpoint", "http://unowned.test/v1"), ("credential_ref", "OPENAI_API_KEY"), ("revoked", True)):
            bad = copy.deepcopy(original)
            bad["processing"]["connections"]["controlled"][field] = value
            with self.assertRaises(ValueError):
                graph.processing_for_endpoint(fixture, bad, endpoint)

    def test_remote_spy_original_namespace_loopback_and_held_identity(self):
        driver_fake = Mock(require=driver.require)
        actual = {"listener": {"nonce": "nonce", "host": "127.0.0.1", "port": 41001,
                  "endpoint": "http://127.0.0.1:41001/v1"}, "provider": {"physical_connections": 1,
                  "error_type": None, "held_ids": [100001]}, "rule_counts": {"rule": 1}, "held_rules": {"100001": "rule"}}
        driver_fake.commands.json.return_value = actual
        proxy = graph.OwnedSpy(driver_fake, "owned-helper", self.root, {"rules": []}, "nonce", time.monotonic()+30)
        self.assertEqual(proxy.endpoint, "http://127.0.0.1:41001/v1")
        self.assertEqual(asyncio.run(proxy.wait_held("rule")), 100001)
        proxy.release(100001)
        command = driver_fake.commands.json.call_args.args[0]
        self.assertEqual(command[:3], ["exec", "owned-helper", "python3"])
        self.assertEqual(command[-2:], ["--physical-call-id", "100001"])
        with self.assertRaises(ValueError):
            proxy.release(100002)
        foreign = copy.deepcopy(actual)
        foreign["listener"]["host"] = "172.22.0.1"
        driver_fake.commands.json.return_value = foreign
        with self.assertRaisesRegex(ValueError, "loopback"):
            proxy.refresh()

    def test_helper_image_identity_refuses_before_any_owned_creation(self):
        fixture = Mock(require=driver.require, report={"docker_root": "/original/daemon"},
                       allocation={"graph_helper_image_id": "sha256:" + "a"*64})
        fixture.commands.json.return_value = [{"Id": "sha256:" + "b"*64,
                                               "Os": "linux", "Architecture": "amd64"}]
        with patch.object(graph.os, "getuid", return_value=1000):
            with self.assertRaisesRegex(ValueError, "image identity"):
                graph.start_owned_spy(fixture, {}, "c"*64, {}, time.monotonic()+30)
        fixture.container.assert_not_called()
        fixture.owned.create.assert_not_called()
        fixture.commands.run.assert_not_called()

    def test_complete_original_claim_quotes_and_coordinates_are_not_clipped(self):
        fixture = Mock(require=driver.require)
        raw_hash = driver.base.digest(b"original quote")
        rendered_hash = driver.base.digest(b"original quote\n")
        recipe = {"source_request": {"source": {"external_id": "key", "hash": raw_hash},
                  "doc": {"text": "original quote"}, "intake": {"revision_id": "r1"}},
                  "rendered_text": "original quote\n", "rendered_sha256": rendered_hash,
                  "expected_assertions": [{"start": 0, "end": 14, "quote": "original quote"}]}
        published = {"source": {"hash": raw_hash}, "doc": {"id": "doc:original"},
                     "immutable_revision": {"id": "doc_revision:original", "doc": "doc:original",
                     "text": "original quote\n", "text_hash": rendered_hash}}
        row = {"kind": "claim", "evidence": [{"source_id": "key", "revision": "r1", "span": {"start": 0, "end": 14},
                    "quote": "original quote", "doc_id": "doc:original", "internal_doc_revision_id": "doc_revision:original",
                    "rendered_span": {"start": 0, "end": 14}, "rendered_quote_sha256": raw_hash,
                    "rendered_text_sha256": rendered_hash}]}
        value = {"results": [copy.deepcopy(row) for _ in range(10)]}
        arithmetic = {"limit20": {"maximum_selected_chunks": 2}}
        graph.verify_small_evidence(fixture, value, [recipe], [published], arithmetic)
        for field, replacement in (("quote", "original"), ("revision", "r2"), ("source_id", "other"),
                                   ("doc_id", "doc:foreign"), ("doc_id", None),
                                   ("internal_doc_revision_id", "doc_revision:other"),
                                   ("rendered_span", {"start": 1, "end": 14}),
                                   ("rendered_quote_sha256", "a"*64), ("rendered_text_sha256", raw_hash)):
            bad = copy.deepcopy(value)
            bad["results"][0]["evidence"][0][field] = replacement
            with self.assertRaises(ValueError):
                graph.verify_small_evidence(fixture, bad, [recipe], [published], arithmetic)
        foreign_parent = copy.deepcopy(published)
        foreign_parent["immutable_revision"]["doc"] = "doc:foreign"
        with self.assertRaises(ValueError):
            graph.verify_small_evidence(fixture, value, [recipe], [foreign_parent], arithmetic)
        # A chunk may include the synthetic final LF in rendered coordinates,
        # while its raw support remains the exact original byte cover.
        chunk = copy.deepcopy(row["evidence"][0])
        chunk["rendered_span"]["end"] = 15
        chunk["rendered_quote_sha256"] = rendered_hash
        graph.verify_evidence(fixture, chunk, recipe, published, claim=False)
        chunk["span"]["start"] = chunk["rendered_span"]["start"] = 14
        with self.assertRaises(ValueError):
            graph.verify_evidence(fixture, chunk, recipe, published, claim=False)

    def test_original_graph_descriptors_match_both_container_and_shared_recipe_consumers(self):
        root = driver.ROOT / "tests/recall_preview/graph-recipes"
        manifest = driver.base.strict_json((root / "manifest.json").read_bytes())
        rows = manifest["recipes"] + manifest["spy_recipes"]
        by_path = {row["path"]: row for row in rows}
        def descriptor(name, canonical=True):
            path = root / name
            result = {"path": str(path.relative_to(driver.ROOT)),
                      "file_sha256": driver.base.digest(path.read_bytes())}
            if canonical:
                # Independent original manifest bytes, not the container codec.
                result["canonical_sha256"] = by_path[name]["canonical_sha256"]
            return result
        plan = {"format": "recall-container-graph-plan/v1", "space": "default",
                "configuration": {"name": "default"}, "positive_query": "Mira",
                "empty_query": "absent-boundary-control", "query_binding_id": "query_embedding",
                "spy": descriptor("container-spy.json"),
                "positive": descriptor("container-positive.json"),
                "response_budget": [descriptor(f"container-response-budget-{index}.json") for index in range(2)],
                "arithmetic": descriptor("container-response-budget-arithmetic.json", False)}
        path = self.root / "graph-plan.json"
        path.write_bytes(driver.base.encoded(plan))
        plan_sha = driver.base.digest(path.read_bytes())
        image = "sha256:" + "a" * 64
        descriptors = [plan["spy"], plan["positive"], *plan["response_budget"], plan["arithmetic"]]
        review = {"graph_plan_sha256": plan_sha, "graph_case_contract": "cb07-two-source-complete-evidence-v1",
                  "graph_helper_image_id": image,
                  "graph_input_sha256": {row["path"]: row["file_sha256"] for row in descriptors}}
        allocation = {"graph_plan": str(path), "graph_plan_sha256": plan_sha, "graph_helper_image_id": image}
        _, inputs = driver.graph_inputs(allocation, review)
        modules = graph.reviewed_modules(Mock(base_root=driver.ROOT, require=driver.require))
        modules["full_workload_spy"].validate_recipe(inputs["spy"], plan["spy"]["canonical_sha256"])
        for recipe, selected in zip([inputs["positive"], *inputs["response_budget"]],
                                    [plan["positive"], *plan["response_budget"]], strict=True):
            modules["full_workload_graph"].validate_recipe(recipe, selected["canonical_sha256"])
        # A report-encoder digest must not be admitted and handed downstream.
        plan["positive"]["canonical_sha256"] = driver.base.digest(driver.base.encoded(inputs["positive"]))
        path.write_bytes(driver.base.encoded(plan))
        allocation["graph_plan_sha256"] = review["graph_plan_sha256"] = driver.base.digest(path.read_bytes())
        with self.assertRaisesRegex(ValueError, "canonical recipe identity"):
            driver.graph_inputs(allocation, review)

    def test_missing_graph_plan_does_not_claim_any_of_the_three_cases(self):
        value = object.__new__(driver.ContainerBoundaryDriver)
        value.graph_plan_inputs = None
        value.unmet = Mock()
        value.graph_cases()
        self.assertEqual([call.args[0] for call in value.unmet.call_args_list], ["CC04", "CB07", "CB09"])
        self.assertIsNone(driver.graph_inputs({}, {}))
        with self.assertRaises(ValueError):
            driver.graph_inputs({"graph_plan_sha256": "a"*64}, {})


class GraphHelperTests(ClosedWorld):
    def helper(self):
        # Only F's helper code executes. G's separately owned modules are
        # replaced with explicit interface doubles, never imported or tested.
        class Capture:
            def __init__(self, directory, maximum):
                self.directory = Path(directory)
                self.directory.mkdir(mode=0o700)
                self.maximum, self.written, self.held = maximum, 0, 0
        transport = types.SimpleNamespace(Capture=Capture, decode=driver.base.strict_json,
            encode=driver.base.encoded, private_new=driver.base.private_new, sha=driver.base.digest)
        spec = importlib.util.spec_from_file_location("container_helper_under_test",
                                                      PATH.with_name("container_graph_helper.py"))
        value = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"container_graph_budget": budget, "full_workload_transport": transport,
                                     "full_workload_spy": types.SimpleNamespace(ControlledSpy=Mock())}):
            spec.loader.exec_module(value)
        return value

    def test_provider_capture_atomically_converts_original_reservation(self):
        helper = self.helper()
        ledger = budget.BudgetLedger.create(self.root / "budget.json", "nonce", calls=0, written=0,
            maximum_calls=5, maximum_bytes=1000, deadline_ns=time.monotonic_ns()+30_000_000_000)
        capture = helper.SharedCapture(self.root / "captures", ledger)
        capture.held = 900
        with patch.object(ledger, "change", wraps=ledger.change) as changes:
            capture.save(100001, b'{"original":true}', {"raw_body": b"original-response"}, 900)
        self.assertEqual(changes.call_count, 1)
        self.assertEqual(changes.call_args.kwargs["held"], (900, 0))
        actual = sum(path.stat().st_size for path in capture.directory.iterdir())
        self.assertEqual(ledger.change()["written"], actual)
        self.assertEqual(capture.written, actual)
        self.assertEqual((capture.directory / "100001.response.bin").read_bytes(), b"original-response")
        with self.assertRaises(ValueError):
            capture.save(100002, b"overflow", {"raw_body": b"overflow"}, 0)

    def test_helper_uses_loopback_and_private_control_then_closes_owned_listener(self):
        helper = self.helper()
        deadline = time.monotonic() + 30
        budget.BudgetLedger.create(self.root / "budget.json", "nonce", calls=0, written=0,
            maximum_calls=5, maximum_bytes=10000, deadline_ns=int(deadline*1e9))
        (self.root / "settings.json").write_bytes(driver.base.encoded({
            "format": "recall-container-graph-helper/v1", "nonce": "nonce", "recipe": {},
            "recipe_sha256": "a"*64, "deadline": deadline, "limits": {}}))
        listener = Mock(sockets=[Mock(getsockname=Mock(return_value=("127.0.0.1", 41001)))])
        listener.wait_closed = AsyncMock()
        control_server = Mock(wait_closed=AsyncMock())
        spy = Mock(failure=None, rule_counts={}, held_rules={})
        spy.snapshot_calls.return_value = {"physical_connections": 0, "error_type": None}
        async def close_spy():
            spy.server.close()
            await spy.server.wait_closed()
        spy.close = AsyncMock(side_effect=close_spy)
        reader = Mock(readline=AsyncMock(return_value=b'{"nonce":"nonce","action":"close"}\n'))
        writer = Mock(drain=AsyncMock(), wait_closed=AsyncMock())
        async def start_control(callback, *, path, limit):
            self.assertEqual(path, str(self.root / "control.sock"))
            self.assertEqual(limit, 4096)
            await callback(reader, writer)
            return control_server
        with patch.object(helper, "ControlledSpy", return_value=spy), \
             patch.object(helper.asyncio, "start_server", new=AsyncMock(return_value=listener)) as provider, \
             patch.object(helper.asyncio, "start_unix_server", side_effect=start_control):
            asyncio.run(helper.serve(self.root))
        self.assertEqual(provider.call_args.args[1:], ("127.0.0.1", 0))
        self.assertEqual(driver.base.strict_json((self.root / "final.json").read_bytes())["listener"]["nonce"], "nonce")
        spy.close.assert_awaited_once()
        listener.close.assert_called_once()
        control_server.close.assert_called_once()
        writer.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
