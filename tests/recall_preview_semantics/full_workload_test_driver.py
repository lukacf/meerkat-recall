"""Closed-world orchestration controls; never an installed product verdict."""
import asyncio
import copy
import importlib.util
from pathlib import Path
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import full_workload_driver as module
from full_workload_driver import Driver, NamedClient, Unmet, accepted_identity, large_text, original_identity, source_request, subset
from full_workload_transport import Capture, decode, encode, sha
from full_workload_test_graph import recipe as graph_recipe, accepted, operation

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("full_workload_cli_for_controls", ROOT / "scripts/qualification/full_workload.py")
CLI = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CLI)
SPACE = "qualification-household"
SECURITY = {"policy_id": "household-policy", "level": 3, "labels": [], "handling": []}
OBSERVED_DOC = "doc:b41aef1b-4a74-4a23-a52f-2f06a0507015"


def publication(original, rendered):
    admission = accepted()
    admission["doc_id"] = OBSERVED_DOC
    admission["operation"]["space_id"] = original["source"]["space_id"]
    revision = original["intake"]["revision_id"]
    admission["operation"]["desired_revision"] = revision
    completed = operation()
    completed.update(desired_revision=revision, published_revision=revision)
    immutable = {"id": "doc_revision:actual-original", "doc": admission["doc_id"],
        **{field: copy.deepcopy(original["source"][field]) for field in ("space_id", "security", "subjects", "subjects_state")},
        "status": "active", "deleted_at": None, "text": rendered, "text_hash": sha(rendered.encode()),
        "renderer": {"name": "elephant.renderer.text", "version": "1.0.0", "config_hash": None},
        "provenance": {"sources": [admission["source_id"]], "chain": [
            {"step": "source_content", "input": admission["source_id"], "output": original["source"]["hash"]}]}}
    return {"accepted": admission, "operation": copy.deepcopy(completed), "head": completed,
            "source": {**copy.deepcopy(original["source"]), "id": admission["source_id"]}, "immutable_revision": immutable}


def cleanup(ordering):
    return {"action": "retract", "idempotency_key": "original-cleanup-intent",
            "original_terminal": {"state": "failed_final", "lifecycle": "active", "error_code": "superseded"},
            "current_head": {"lifecycle": "retracted", "state": "complete", "published_revision": None},
            "no_later_original_physical_sends": True, ordering: True}


class Spy:
    failure = None
    def __init__(self):
        self.count, self.closed = 0, 0
        self.calls, self.rule_counts = [], {}
        self.recipe = {"rules": [{"id": "original-extract", "space": SPACE, "mode": "structured", "required_text": ["Mira"], "hold_calls": [1]}]}
    def snapshot_calls(self):
        return {"physical_connections": self.count, "active": 0, "held_ids": [], "error_type": None, "complete_calls": self.calls}
    async def close(self):
        self.closed += 1


class Client:
    def __init__(self, capture):
        self.capture, self.count = capture, 0
        self.clock, self.deadline, self.expires = time.monotonic, time.monotonic() + 10, time.time() + 10
        self.responses, self.requests = [], []
        self.limits = {"public_http_calls_per_backend": 8000}
    async def request(self, method, path, space, body=None, *, role, **options):
        self.count += 1
        self.requests.append((method, path, space, copy.deepcopy(body), role, options))
        if not self.responses:
            raise AssertionError("unexpected unreviewed mock request")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return {"id": self.count, "status": 200, "framing_complete": True, **response}


class DriverTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        for name in ("asyncio.start_server", "asyncio.open_connection", "subprocess.Popen", "subprocess.run", "socket.create_connection"):
            guard = patch(name, side_effect=AssertionError("closed-world driver controls cannot execute runtime"))
            guard.start()
            self.addCleanup(guard.stop)
        directory = tempfile.TemporaryDirectory(prefix="recall-full-driver-control-")
        self.addCleanup(directory.cleanup)
        self.capture = Capture(Path(directory.name) / "capture", 32 * 1024**2)
        self.client, self.spy = Client(self.capture), Spy()

    def driver(self):
        value = Driver.__new__(Driver)
        value.client, value.spy = self.client, self.spy
        value.plan = {"cases": {key: {} for key in module.CASES}, "graph_recipes": {}, "graph_recipe_sha256": {}}
        value.contract, value.corpus = {}, {}
        value.plan_sha256, value.backend, value.nonce = "f" * 64, "elephant-db", "original-run"
        value.external_receipts, value.service_control, value.capacity_capture = {}, None, None
        value.ob3_oracle = None
        value.runtime_inspection = None
        value.inputs, value.accepted, value.published, value.graphs, value.held = {}, {}, {}, {}, {}
        value.graph_publications = {}
        value.coordinate_cache, value.fenced_rules = {}, {}
        # Subcases share this task-private capture; never reuse an event identity.
        last = max((int(path.stem.split("-")[1]) for path in self.capture.directory.glob("event-*.json")), default=0)
        value.results, value.events, value.event_count, value.current_case, value.stopped = [], [], last, None, False
        return value

    def test_literal_oracle_is_typed_and_does_not_learn_candidate_output(self):
        self.assertTrue(subset({"state": "complete", "other": "captured"}, {"state": "complete"}))
        self.assertFalse(subset({"count": True}, {"count": 1}))
        self.assertFalse(subset({"count": 1.0}, {"count": 1}))
        self.assertFalse(subset({"state": "pending"}, {"state": "complete"}))

    def test_original_large_recipe_and_typed_request_are_exact(self):
        contract = decode((ROOT / "tests/recall_preview/full-workload-contract.json").read_bytes())
        text = large_text(contract)
        self.assertEqual(len(text.encode()), 2_000_000)
        self.assertEqual(sha(text.encode()), contract["full_fixture"]["large_recipe"]["sha256"])
        row = {"space": SPACE, "key": "full/large", "revision": "original-r1", "text": text, "sha256": sha(text.encode())}
        first = source_request(row, "elephant-db", "original-run", SECURITY)
        self.assertEqual(first, source_request(row, "elephant-db", "original-run", SECURITY))
        self.assertEqual(first["source"]["security"], SECURITY)
        self.assertEqual(first["doc"]["security"], SECURITY)
        self.assertEqual(first["doc"]["text"], text)
        self.assertNotIn("expected_generation", first["intake"])
        self.assertNotEqual(first["intake"]["idempotency_key"], source_request(row, "surreal-db", "original-run", SECURITY)["intake"]["idempotency_key"])
        contract["full_fixture"]["large_recipe"]["sha256"] = "0" * 64
        with self.assertRaises(Unmet):
            large_text(contract)

    def test_query_evidence_binds_original_bytes_revision_and_actual_immutable_identity(self):
        driver = self.driver()
        original = graph_recipe()["source_request"]
        driver.inputs["original"] = original
        driver.published["original"] = publication(original, original["doc"]["text"] + "\n")
        key, revision = original["source"]["external_id"], original["intake"]["revision_id"]
        raw = original["doc"]["text"]
        request = {"source_revision_constraints": {key: revision}}
        evidence = {"source_id": key, "revision": revision, "span": {"start": 0, "end": len(raw.encode())},
                    "quote": raw, "internal_doc_revision_id": "doc_revision:actual-original", "doc_id": OBSERVED_DOC,
                    "rendered_span": {"start": 0, "end": len(raw.encode())}, "rendered_quote_sha256": sha(raw.encode()),
                    "rendered_text_sha256": sha((raw + "\n").encode())}
        response = {"results": [{"kind": "claim", "text": "independent claim", "evidence": [evidence], "score": 0.5}],
                    "coverage": {key: {"published_revision": revision, "state": "complete"}}, "partial": False}
        expected = [{"source_key": key, "revision": revision, "quote": raw, "quote_hash": sha(raw.encode())}]
        driver.query_evidence(SPACE, request, response, expected, expected)
        chunk_only = copy.deepcopy(response)
        chunk_only["results"][0]["kind"] = "chunk"
        driver.query_evidence(SPACE, request, chunk_only, expected)
        with self.assertRaises(Unmet):
            driver.query_evidence(SPACE, request, chunk_only, expected, expected)
        for field, value in (("source_id", "foreign"), ("revision", "foreign"), ("quote", "forged"),
                             ("internal_doc_revision_id", "doc_revision:other"), ("span", {"start": False, "end": len(raw.encode())})):
            bad = copy.deepcopy(response)
            bad["results"][0]["evidence"][0][field] = value
            with self.subTest(field=field), self.assertRaises(Unmet):
                driver.query_evidence(SPACE, request, bad, expected)
        with self.assertRaises(Unmet):
            driver.query_evidence(module.SPACES[1], request, response, expected)
        with self.assertRaises(Unmet):
            driver.query_evidence(SPACE, request, {**response, "results": []}, expected)
        # Even an extra non-required hit must have exact original evidence.
        bad = copy.deepcopy(response)
        bad["results"].append(copy.deepcopy(bad["results"][0]))
        bad["results"][1]["evidence"][0]["source_id"] = "foreign"
        with self.assertRaises(Unmet):
            driver.query_evidence(SPACE, request, bad, expected)

    def coordinate_case(self, literal):
        driver = self.driver()
        original = graph_recipe()["source_request"]
        original["doc"]["text"] = literal["accepted_text"]
        original["source"]["hash"] = literal["accepted_utf8_sha256"]
        driver.inputs["original"] = original
        driver.published["original"] = publication(original, literal["rendered_text"])
        key, revision = original["source"]["external_id"], original["intake"]["revision_id"]
        request = {"source_revision_constraints": {key: revision}}
        evidence = {"source_id": key, "revision": revision, "span": copy.deepcopy(literal["raw_span"]),
                    "quote": literal["raw_quote"], "internal_doc_revision_id": "doc_revision:actual-original", "doc_id": OBSERVED_DOC,
                    "rendered_span": copy.deepcopy(literal["rendered_span"]),
                    "rendered_quote_sha256": literal["rendered_quote_sha256"],
                    "rendered_text_sha256": literal["rendered_utf8_sha256"]}
        response = {"results": [{"kind": "claim", "text": "Independent claim", "score": 1.0, "evidence": [evidence]}],
                    "coverage": {key: {"published_revision": revision, "state": "complete"}}, "partial": False}
        expected = [{"source_key": key, "revision": revision, "quote": literal["raw_quote"], "quote_hash": literal["raw_quote_sha256"]}]
        return driver, request, response, expected

    def coordinate_literals(self):
        raw = (ROOT / "tests/recall_preview/render-coordinate-fixtures.json").read_bytes()
        self.assertEqual(sha(raw), "54138704a332d6e275dc434ad0d6456f49107a811c08ea9abbafdbd68171dc3b")
        return {row["id"]: row for row in decode(raw)["positive_cases"]}

    def test_seven_independent_raw_rendered_coordinate_literals(self):
        for identifier, literal in self.coordinate_literals().items():
            with self.subTest(identifier=identifier):
                driver, request, response, expected = self.coordinate_case(literal)
                driver.query_evidence(SPACE, request, response, expected, expected)
                self.assertEqual(self.client.count, 0)

    def test_evidence_parent_document_is_required_and_bound_to_original_publication(self):
        literal = self.coordinate_literals()["identity_without_synthetic_lf"]
        other_doc = "doc:fd8730ca-32e4-42eb-b0c3-41386c8dfeee"
        for kind in ("claim", "entity", "chunk"):
            driver, request, response, expected = self.coordinate_case(literal)
            response["results"][0]["kind"] = kind
            driver.query_evidence(SPACE, request, response, expected)
        for mutation in ("missing", "alias", "extra-alias", "other-doc", "changed-parent", "false", "uppercase", "wrong-table"):
            driver, request, response, expected = self.coordinate_case(literal)
            evidence = response["results"][0]["evidence"][0]
            if mutation == "missing": evidence.pop("doc_id")
            elif mutation == "alias": evidence["internal_doc_id"] = evidence.pop("doc_id")
            elif mutation == "extra-alias": evidence["internal_doc_id"] = evidence["doc_id"]
            elif mutation == "other-doc": evidence["doc_id"] = other_doc
            elif mutation == "changed-parent":
                evidence["doc_id"] = other_doc
                driver.published["original"]["immutable_revision"]["doc"] = other_doc
            elif mutation == "false": evidence["doc_id"] = False
            elif mutation == "uppercase": evidence["doc_id"] = OBSERVED_DOC.upper()
            elif mutation == "wrong-table": evidence["doc_id"] = OBSERVED_DOC.replace("doc:", "source:")
            with self.subTest(mutation=mutation), self.assertRaises(Unmet):
                driver.query_evidence(SPACE, request, response, expected)
        self.assertEqual(self.client.count, 0)

    def test_equal_quote_wrong_occurrence_and_generated_only_support_refuse(self):
        literals = self.coordinate_literals()
        for identifier, field, value in (
            ("second_repeated_phrase_after_crlf_trim", "span", {"start": 0, "end": 22}),
            ("unicode_after_lone_cr_and_crlf", "rendered_span", {"start": 6, "end": 22}),
            ("support_with_synthetic_terminal_lf", "rendered_span", {"start": 22, "end": 23}),
        ):
            driver, request, response, expected = self.coordinate_case(literals[identifier])
            evidence = response["results"][0]["evidence"][0]
            evidence[field] = value
            # Even a self-consistent candidate slice digest cannot repair wrong origins.
            rendered = literals[identifier]["rendered_text"].encode()
            evidence["rendered_quote_sha256"] = sha(rendered[evidence["rendered_span"]["start"]:evidence["rendered_span"]["end"]])
            with self.subTest(identifier=identifier), self.assertRaises(Unmet):
                driver.query_evidence(SPACE, request, response, expected)

    def test_complete_immutable_renderer_source_and_publication_proof_are_required(self):
        literal = self.coordinate_literals()["identity_without_synthetic_lf"]
        for mutation in ("renderer", "config", "raw", "rendered", "publication", "coverage", "hash-format", "foreign-revision"):
            driver, request, response, expected = self.coordinate_case(literal)
            observed = driver.published["original"]
            evidence = response["results"][0]["evidence"][0]
            if mutation == "renderer": observed["immutable_revision"]["renderer"]["version"] = "unknown"
            elif mutation == "config": observed["immutable_revision"]["renderer"]["config_hash"] = "unknown"
            elif mutation == "raw": driver.inputs["original"]["doc"]["text"] += "Changed"
            elif mutation == "rendered":
                observed["immutable_revision"]["text"] += "Changed"
                digest = sha(observed["immutable_revision"]["text"].encode())
                observed["immutable_revision"]["text_hash"] = evidence["rendered_text_sha256"] = digest
            elif mutation == "publication": observed["operation"]["state"] = "pending"
            elif mutation == "coverage": next(iter(response["coverage"].values()))["state"] = "pending"
            elif mutation == "hash-format": evidence["rendered_quote_sha256"] = evidence["rendered_quote_sha256"].upper()
            elif mutation == "foreign-revision": observed["immutable_revision"]["space_id"] = module.SPACES[1]
            with self.subTest(mutation=mutation), self.assertRaises(Unmet):
                driver.query_evidence(SPACE, request, response, expected)

    async def test_actual_load_helper_invokes_full_driver_proof_for_all_original_280_intents(self):
        from full_workload_test_load import Client as LoadClient, plan as load_plan, requirements
        original_plan = load_plan()
        expected = requirements(original_plan)
        driver = self.driver()
        responses = {}
        for space in module.SPACES:
            original = graph_recipe()["source_request"]
            original["source"].update(space_id=space, external_id="original-source")
            original["doc"]["space_id"] = space
            driver.inputs[space] = original
            driver.published[space] = publication(original, original["doc"]["text"] + "\n")
            raw = original["doc"]["text"].encode()
            responses[space] = {"source_id": "original-source", "revision": "original-r1", "quote": raw.decode(),
                "span": {"start": 0, "end": len(raw)}, "internal_doc_revision_id": "doc_revision:actual-original", "doc_id": OBSERVED_DOC,
                "rendered_span": {"start": 0, "end": len(raw)}, "rendered_quote_sha256": sha(raw),
                "rendered_text_sha256": sha(raw + b"\n")}
        class OriginalClient(LoadClient):
            async def request(self, method, path, space, body, **options):
                result = await super().request(method, path, space, body, **options)
                result["body"]["results"][0]["evidence"] = [copy.deepcopy(responses[space])]
                return result
        client = OriginalClient(self.spy)
        client.capture, client.expires = self.capture, time.time() + 20
        driver.client = client
        driver.large_graph_capacity, driver.quiescent = AsyncMock(), AsyncMock()
        driver.plan.update(load_plan=original_plan, load_plan_sha256=sha(encode(original_plan)))
        driver.plan["cases"]["FW12"] = {"query_requirements": expected, "query_requirements_sha256": sha(encode(expected))}
        with patch.object(driver, "query_evidence", wraps=driver.query_evidence) as verifier:
            await driver.fw12()
        self.assertEqual(verifier.call_count, 280)
        self.assertEqual(client.count, 280)
        self.assertEqual(self.spy.count, 0)
        # A different independently valid map cannot borrow the original map pin.
        driver.plan["cases"]["FW12"]["query_requirements_sha256"] = "0" * 64
        with self.assertRaises(Unmet):
            await driver.fw12()
        self.assertEqual(client.count, 280)

    async def test_named_credentials_share_one_client_budget_without_mutation(self):
        tokens = {f"{space}:data": "one.two.three" for space in module.SPACES}
        client = NamedClient("http://127.0.0.1:8123", tokens, self.capture, {}, time.monotonic() + 2, time.time() + 2)
        received = []
        async def original(this, method, path, space, body, *, role, **options):
            this.count += 1
            received.append((id(this), space, role, this.tokens.copy()))
            await asyncio.sleep(0)
            return {"id": this.count}
        with patch.object(module.Client, "request", original):
            await asyncio.gather(*[client.request("GET", "/v1/spaces/" + space, space, role="data") for space in module.SPACES])
        self.assertEqual(client.count, 2)
        self.assertEqual({row[0] for row in received}, {id(client)})
        self.assertEqual([row[2] for row in received], [space + ":data" for space in module.SPACES])
        self.assertTrue(all(row[3] == tokens for row in received))
        with self.assertRaises(Unmet):
            await client.request("GET", "/v1/spaces/other", "other", role="data")

    async def test_uncertain_original_source_submission_is_never_retried(self):
        driver = self.driver()
        self.client.responses = [OSError("bounded mock disconnect")]
        request = graph_recipe()["source_request"]
        with self.assertRaises(OSError):
            await driver.accept("original", request)
        with self.assertRaises(Unmet):
            await driver.accept("original", request)
        self.assertEqual(self.client.count, 1)
        self.assertEqual(driver.inputs["original"], request)
        self.assertNotIn("original", driver.accepted)
        intents = [decode(path.read_bytes()) for path in sorted(self.capture.directory.glob("event-*.json"))]
        self.assertEqual(intents[0]["kind"], "original-source-intent")
        self.assertEqual(intents[0]["value"]["request"], request)

    async def test_graph_generation_binding_retains_original_template_and_actual_head(self):
        driver = self.driver()
        template = graph_recipe()
        original = copy.deepcopy(template)
        driver.plan["graph_recipes"]["held"] = template
        driver.plan["graph_recipe_sha256"]["held"] = sha(encode(template))
        self.client.responses = [{"body": operation()}]
        fixture = await driver.fixture("held", replace=True)
        self.assertEqual(template, original)
        self.assertNotIn("expected_generation", template["source_request"]["intake"])
        self.assertEqual(fixture.request["intake"]["expected_generation"], "generation-1")
        self.assertEqual(fixture.recipe["expected_assertions"], template["expected_assertions"])
        events = [decode(path.read_bytes()) for path in sorted(self.capture.directory.glob("event-*.json"))]
        self.assertEqual(events[0]["value"]["head"], operation())
        self.assertEqual(events[1]["value"]["reviewed_template_sha256"], sha(encode(original)))
        self.assertNotEqual(events[1]["value"]["wire_recipe_sha256"], sha(encode(original)))
        with self.assertRaises(Unmet):
            await driver.fixture("held", replace=True)
        self.assertEqual(self.client.count, 1)

    async def test_substituted_generation_fails_before_graph_mutation(self):
        driver = self.driver()
        template = graph_recipe()
        driver.plan["graph_recipes"]["held"] = template
        driver.plan["graph_recipe_sha256"]["held"] = sha(encode(template))
        self.client.responses = [{"body": operation()}]
        with self.assertRaises(Unmet):
            await driver.fixture("held", replace=True, generation="foreign-current-head")
        self.assertEqual(len(self.client.requests), 1)
        self.assertEqual(self.client.requests[0][0], "GET")
        self.assertEqual(driver.graphs, {})

    def test_accepted_and_polling_identity_use_actual_distinct_wire_shapes(self):
        request = graph_recipe()["source_request"]
        original = accepted_identity(request, accepted())
        original_identity(original, operation(), "original-r1")
        for field in ("operation_id", "generation", "internal_source_id", "desired_revision"):
            changed = operation()
            changed[field] = "substituted"
            with self.subTest(field=field), self.assertRaises(Unmet):
                original_identity(original, changed, "original-r1")
        changed = accepted()
        changed["operation"]["generation"] = changed["operation"].pop("source_generation")
        with self.assertRaises(Unmet):
            accepted_identity(request, changed)

    async def test_completed_noop_keeps_original_identity_and_current_status_without_new_work(self):
        driver = self.driver()
        label, request, first = "original", graph_recipe()["source_request"], accepted()
        first["operation"].update(state="accepted", accepted_at="original-time", published_revision=None)
        driver.inputs[label], driver.accepted[label], driver.published[label] = request, first, {"accepted": first}
        driver.plan["cases"]["FW05"] = {"source_labels": [label]}
        retry = copy.deepcopy(first)
        retry["work_item_id"] = None
        retry["operation"].update(state="completed", completed_at="actual-completion", published_revision="original-r1", error_code=None)
        self.client.responses = [{"body": operation()}, {"body": operation()}, {"status": 202, "body": retry}, {"body": operation()}, {"body": operation()}]
        await driver.fw05()
        self.assertEqual(self.client.count, 5)
        self.assertEqual(self.client.requests[2][3], request)
        self.assertEqual(driver.accepted[label]["operation"]["state"], "accepted")
        self.assertEqual(self.spy.count, 0)

    async def test_completed_noop_new_work_or_generation_does_not_pass(self):
        for key in ("new-work", "new-generation"):
            driver = self.driver()
            label, request, first = "original", graph_recipe()["source_request"], accepted()
            first["operation"].update(state="accepted", accepted_at="original-time", published_revision=None)
            driver.inputs[label], driver.accepted[label], driver.published[label] = request, first, {"accepted": first}
            driver.plan["cases"]["FW05"] = {"source_labels": [label]}
            retry = copy.deepcopy(first)
            retry["work_item_id"] = "work_item:new" if key == "new-work" else None
            retry["operation"].update(state="completed", published_revision="original-r1", error_code=None)
            if key == "new-generation":
                retry["operation"]["source_generation"] = "new-generation"
            self.client.responses = [{"body": operation()}, {"body": operation()}, {"status": 202, "body": retry}]
            with self.subTest(key=key), self.assertRaises(Unmet):
                await driver.fw05()

    async def test_failed_phase_records_all_remaining_cases_and_owned_cleanup(self):
        driver = self.driver()
        called = []
        async def first():
            called.append("FW01")
            raise Unmet("actual prerequisite unavailable")
        driver.fw01 = first
        for identifier in module.CASES[1:]:
            setattr(driver, identifier.lower(), AsyncMock(side_effect=AssertionError("dependent phase must not execute")))
        result = await driver.run()
        self.assertEqual(called, ["FW01"])
        self.assertEqual(len(result["cases"]), 14)
        self.assertEqual(result["cases"][0]["status"], "unmet")
        self.assertTrue(all(row["status"] == "unexecuted" for row in result["cases"][1:]))
        self.assertEqual(self.spy.closed, 1)
        self.assertFalse(result["qualified"])
        self.assertFalse(result["product_acceptance"])

    async def test_expired_original_clock_never_dispatches_and_still_closes_owned_spy(self):
        driver = self.driver()
        self.client.deadline = self.client.clock() - 1
        result = await driver.run()
        self.assertEqual(self.client.count, 0)
        self.assertEqual(self.spy.closed, 1)
        self.assertEqual(result["cases"][0]["status"], "unmet")
        self.assertTrue(all(row["status"] == "unexecuted" for row in result["cases"][1:]))

    def test_external_consumer_is_original_evidence_not_local_acceptance(self):
        driver = self.driver()
        self.assertFalse(driver.external("FW14")["receipt_imported"])
        original = {"receipt_version": "ob3-recall-outage-1", "run_id": "ob3-original",
                    "ob3_git_sha": "a" * 40, "recall_contract_version": "v1",
                    "recall_endpoint_state": "refused", "batch": {"kind": "review"},
                    "expected_participating_workers": ["original-worker"], "participating_workers": [],
                    "read_total": {"reads": 2, "evidence": 0, "fallbacks": {"refused": 2}, "size_retries": 0},
                    "workers": {}, "por_push": {}, "verdict": {"status": "INCONCLUSIVE"}}
        driver.external_receipts["FW14"] = {"owner": "OB3", "case": "FW14", "backend": driver.backend,
            "run_nonce": driver.nonce, "original_sha256": sha(encode(original)), "original": original}
        def retain_actual_bytes():
            raw = encode(original) + b"\n"
            driver.external_receipts["FW14"].update(original_raw=raw, original_sha256=sha(raw))
        self.assertFalse(driver.external("FW14")["receipt_imported"])
        retain_actual_bytes()
        imported = driver.external("FW14")
        self.assertTrue(imported["receipt_imported"])
        self.assertEqual(imported["status"], "unmet")
        original["recall_endpoint_state"] = "unavailable"
        retain_actual_bytes()
        self.assertTrue(driver.external("FW14")["receipt_imported"])
        original["read_total"]["reads"] = 0
        # Raw custody can import failed/zero-read evidence. A producer PASS
        # label still cannot authorize a local consumer verdict.
        original["verdict"]["status"] = "PASS"
        retain_actual_bytes()
        self.assertEqual(driver.external("FW14")["status"], "unmet")
        self.assertTrue(driver.external("FW14")["consumer_accounting"].startswith("unmet:"))
        original["total_read"] = original.pop("read_total")
        with self.assertRaises(Unmet):
            driver.external("FW14")
        original["read_total"] = original.pop("total_read")
        original["read_total"]["recall_reads_attempted"] = original["read_total"].pop("reads")
        with self.assertRaises(Unmet):
            driver.external("FW14")

    async def test_public_cleanup_keeps_original_terminal_distinct_and_fences_future_dispatch(self):
        driver = self.driver()
        fixture = SimpleNamespace(space=SPACE, request=graph_recipe()["source_request"], accepted=accepted())
        driver.graphs["failed"] = fixture
        driver.plan["graph_recipes"]["failed"] = graph_recipe()
        self.spy.rule_counts["original-extract"] = 1
        before = operation("failed_retryable")
        before["error_code"] = "provider_failed"
        terminal = operation("failed_final")
        terminal["error_code"] = "superseded"
        retracted = {**terminal, "generation": "generation-cleanup", "operation_id": "operation:cleanup",
                     "lifecycle": "retracted", "state": "complete", "published_revision": None, "error_code": None}
        self.client.responses = [{"body": before}, {"status": 202, "body": retracted}, {"body": terminal}, {"body": retracted}]
        observed = await driver.retract_original("failed", cleanup("before_release_of_running"), 1)
        self.assertEqual(observed["original_operation"]["generation"], "generation-1")
        self.assertEqual(observed["current_head"]["generation"], "generation-cleanup")
        self.assertEqual(self.client.requests[1][0], "POST")
        self.assertEqual(self.client.requests[1][3], {"expected_generation": "generation-1"})
        self.assertEqual(self.client.requests[1][5]["idempotency"], "original-cleanup-intent")
        driver.check()
        self.spy.rule_counts["original-extract"] += 1
        with self.assertRaises(Unmet):
            driver.check()
        self.assertEqual(self.client.count, 4)

    async def test_cleanup_refuses_changed_current_source_before_any_mutation(self):
        driver = self.driver()
        fixture = SimpleNamespace(space=SPACE, request=graph_recipe()["source_request"], accepted=accepted())
        driver.graphs["failed"] = fixture
        driver.plan["graph_recipes"]["failed"] = graph_recipe()
        self.spy.rule_counts["original-extract"] = 1
        changed = operation("failed_retryable")
        changed["generation"] = "another-source-intent"
        self.client.responses = [{"body": changed}]
        with self.assertRaises(Unmet):
            await driver.retract_original("failed", cleanup("before_release_of_running"), 1)
        self.assertEqual([row[0] for row in self.client.requests], ["GET"])

    async def test_backlog_runtime_inspection_surrounds_both_named_cleanup_sequences(self):
        driver, order = self.driver(), []
        cases = [{"space": space} for space in module.SPACES]
        driver.space_cases = lambda case: cases
        async def inspect(boundary):
            order.append(boundary)
        async def completed_case(case):
            order.append((case["space"], "original-cleanup-completed"))
        driver.runtime_inspection = AsyncMock(side_effect=inspect)
        driver._fw09 = AsyncMock(side_effect=completed_case)
        await driver.fw09()
        self.assertEqual(order, ["before_fw09_first_space", (module.SPACES[0], "original-cleanup-completed"),
                                 "before_fw09_second_space", (module.SPACES[1], "original-cleanup-completed"),
                                 "after_fw09_cleanup"])
        self.assertEqual(driver.runtime_inspection.await_count, 3)
        self.assertEqual(driver._fw09.await_count, 2)
        self.assertEqual(self.client.count, 0)
        # This proves adapter ordering only; C's actual inspector separately
        # validates the original process and captured effective recipes.

    async def test_backlog_missing_runtime_inspector_refuses_before_any_fixture(self):
        driver = self.driver()
        driver.space_cases = lambda case: [{"space": space} for space in module.SPACES]
        driver._fw09 = AsyncMock(side_effect=AssertionError("no backlog without actual runtime custody"))
        with self.assertRaisesRegex(Unmet, "original effective worker runtime inspection is unavailable"):
            await driver.fw09()
        driver._fw09.assert_not_awaited()
        self.assertEqual(self.client.count, 0)
        self.assertEqual(driver.events, [])

    async def test_original_runtime_mismatch_stops_dependent_cases_at_every_boundary(self):
        for completed, rejected_boundary in enumerate(("before_fw09_first_space", "before_fw09_second_space", "after_fw09_cleanup")):
            with self.subTest(boundary=rejected_boundary):
                driver = self.driver()
                for identifier in module.CASES:
                    if identifier != "FW09":
                        setattr(driver, identifier.lower(), AsyncMock(return_value=None))
                driver.space_cases = lambda case: [{"space": space} for space in module.SPACES]
                driver._fw09 = AsyncMock(return_value=None)
                async def inspect(boundary):
                    if boundary == rejected_boundary:
                        raise Unmet("original effective recipe or process changed")
                driver.runtime_inspection = AsyncMock(side_effect=inspect)
                result = await driver.run()
                self.assertEqual(result["cases"][8]["status"], "unmet")
                self.assertTrue(all(row["status"] == "unexecuted" for row in result["cases"][9:]))
                self.assertEqual(driver._fw09.await_count, completed)
                driver.fw11.assert_not_awaited()
                self.assertFalse(result["product_acceptance"])

    async def test_runtime_inspection_does_not_accept_a_boolean_or_startup_receipt(self):
        for substitute in (True, False, {"workers_enabled": True}, {"qualified": True}):
            with self.subTest(substitute=substitute):
                driver = self.driver()
                driver.space_cases = lambda case: [{"space": space} for space in module.SPACES]
                driver.runtime_inspection = AsyncMock(return_value=substitute)
                driver._fw09 = AsyncMock(side_effect=AssertionError("a receipt is not an actual inspection"))
                with self.assertRaises(Unmet):
                    await driver.fw09()
                driver._fw09.assert_not_awaited()
                self.assertEqual(driver.events, [])
                self.assertEqual(self.client.count, 0)

    async def test_runtime_inspection_cannot_extend_original_workload_deadline(self):
        driver = self.driver()
        driver.space_cases = lambda case: [{"space": space} for space in module.SPACES]
        async def inspect(boundary):
            self.client.deadline = self.client.clock() - 1
        driver.runtime_inspection = AsyncMock(side_effect=inspect)
        driver._fw09 = AsyncMock(side_effect=AssertionError("expired original allocation"))
        with self.assertRaises(Unmet):
            await driver.fw09()
        driver.runtime_inspection.assert_awaited_once_with("before_fw09_first_space")
        driver._fw09.assert_not_awaited()
        self.assertEqual(driver.events, [])
        self.assertEqual(self.client.count, 0)

    async def test_backlog_failure_is_observed_first_and_retracted_before_release(self):
        driver, order = self.driver(), []
        fixtures = {}
        for label in ("failed", "running", "pending", "intake"):
            fixtures[label] = SimpleNamespace(space=SPACE, request={"label": label}, accepted={"label": label})
            async def accept_original(label=label): order.append(("accept", label))
            fixtures[label].accept = accept_original
        async def fixture(label, **options):
            driver.graphs[label] = fixtures[label]
            order.append(("prepare", label))
            return fixtures[label]
        async def held(label):
            order.append(("held", label))
            return await fixture(label)
        async def observe(request, admission, **options):
            label = request["label"]
            order.append(("operation", label))
            return {"state": {"failed": "failed_retryable", "running": "running", "pending": "pending"}[label]}
        async def retract(label, contract, count): order.append(("retract", label))
        async def complete(label): order.append(("complete", label)); return {"label": label}
        driver.fixture, driver.held_fixture, driver.operation = fixture, held, observe
        driver.original_rule_count = lambda recipe: ("original-extract", 1)
        self.spy.rule_counts["original-extract"] = 1
        driver.retract_original, driver.complete_graph = retract, complete
        driver.release = lambda label: order.append(("release", label))
        driver.query, driver.quiescent = AsyncMock(), AsyncMock()
        case = {"failed_recipe": "failed", "running_recipe": "running", "pending_recipe": "pending", "intake_recipe": "intake",
                "observe_seconds": 1, "queries": [], "required_observation_order": ["failed_retryable", "running", "pending"],
                "failed_cleanup": cleanup("before_release_of_running")}
        await driver._fw09(case)
        self.assertLess(order.index(("operation", "failed")), order.index(("held", "running")))
        self.assertLess(order.index(("retract", "failed")), order.index(("release", "running")))
        self.assertNotIn(("complete", "failed"), order)
        self.assertEqual([label for action, label in order if action == "complete"], ["running", "pending", "intake"])

    async def test_provider_failure_cleanup_precedes_restoration_and_preserves_actual_category(self):
        for state in ("failed_retryable", "failed_final"):
            driver, order = self.driver(), []
            fixture = SimpleNamespace(space=SPACE, request=graph_recipe()["source_request"], accepted=accepted())
            driver.graphs["failed"] = fixture
            driver.plan["graph_recipes"]["failed"] = graph_recipe()
            self.spy.recipe["rules"][0]["statuses"] = [503]
            self.spy.rule_counts["original-extract"] = 0
            async def held(label):
                self.spy.rule_counts["original-extract"] = 1
                return fixture
            driver.held_fixture, driver.quiescent = held, AsyncMock()
            driver.release = lambda label: order.append("release")
            initial = {"processing": {"generation": 1, "index_generation": 3, "bindings": {"extraction": {"revoked": False}}}}
            revoked = copy.deepcopy(initial)
            revoked["processing"]["generation"] = 2
            revoked["processing"]["bindings"]["extraction"]["revoked"] = True
            basis = copy.deepcopy(revoked)
            basis["processing"]["index_generation"] = 4
            restored = copy.deepcopy(basis)
            restored["processing"]["generation"] = 3
            restored["processing"]["bindings"]["extraction"]["revoked"] = False
            self.client.responses = [{"body": initial}, {"body": {}}, {"body": revoked}, {"body": basis}, {"body": {}}, {"body": restored}]
            original_exchange = driver.exchange
            async def exchange(method, path, space, body=None, **options):
                if method == "PUT": order.append("revoke" if body["processing"]["bindings"]["extraction"]["revoked"] else "restore")
                return await original_exchange(method, path, space, body, **options)
            driver.exchange = exchange
            clock = [0.0]
            self.client.clock, self.client.deadline = lambda: clock[0], 100.0
            async def pause(*args): clock[0] += 1
            driver.pause = pause
            async def observe(*args, **options):
                return {**operation(state if "cleanup" not in order else "failed_final"), "error_code": "provider_binding_revoked" if "cleanup" not in order else "superseded"}
            driver.operation = observe
            async def retract(*args): order.append("cleanup"); return {}
            driver.retract_original = retract
            case = {"recipe": "failed", "revoke_kind": "bindings", "revoke_id": "extraction", "observe_seconds": 1,
                    "allowed_original_failure_states": ["failed_retryable", "failed_final"], "residual_cleanup": cleanup("before_binding_restore")}
            await driver._fw11(case)
            self.assertLess(order.index("cleanup"), order.index("restore"))
            self.assertEqual([item for item in order if item in ("revoke", "release", "cleanup", "restore")], ["revoke", "release", "cleanup", "restore"])
            self.assertEqual(self.spy.rule_counts["original-extract"], 1)
            puts = [row[3]["processing"] for row in self.client.requests if row[0] == "PUT"]
            self.assertEqual(puts[-2:], [revoked["processing"], restored["processing"]])

    def test_ob3_adapter_passes_exact_raw_pair_to_independent_oracle_without_acceptance(self):
        driver = self.driver()
        original = {"receipt_version": "ob3-recall-outage-2", "run_id": "original", "ob3_git_sha": "a" * 40,
                    "recall_contract_version": "recall-preview-1", "recall_endpoint_state": "refused", "batch": {},
                    "expected_participating_workers": [], "participating_workers": [], "read_total": {
                        "reads": 0, "evidence": 0, "fallbacks": {}, "size_retries": 0}, "workers": {}, "por_push": {}, "verdict": {}}
        raw, expected_raw = encode(original) + b" \n", b'{ "original_independent_fixture": true }\n'
        driver.external_receipts["FW14"] = {"owner": "OB3", "case": "FW14", "backend": driver.backend, "run_nonce": driver.nonce,
            "original": original, "original_raw": raw, "original_sha256": sha(raw),
            "expectations_raw": expected_raw, "expectations_sha256": sha(expected_raw)}
        # The mock here qualifies only adapter byte forwarding. D separately
        # owns actual accounting controls; this synthetic receipt proves none.
        scoped = {"scope": "external_receipt_consistency", "product_acceptance": False, "verdict": "pass",
                  "receipt_sha256": sha(raw), "expectations_sha256": sha(expected_raw)}
        oracle = Mock(return_value=scoped)
        driver.ob3_oracle = oracle
        result = driver.external("FW14")
        oracle.assert_called_once_with(raw, expected_raw)
        self.assertEqual(result["status"], "unmet")
        self.assertFalse(result["product_acceptance"])
        retained = sorted(self.capture.directory.glob("external-*.json"))
        self.assertEqual([path.read_bytes() for path in retained], [raw, expected_raw])
        driver.ob3_oracle = None
        with self.assertRaisesRegex(Unmet, "original verified OB3 oracle callable is unavailable"):
            driver.external("FW14")
        oracle.assert_called_once_with(raw, expected_raw)
        driver.external_receipts["FW14"]["original"]["run_id"] = "changed"
        with self.assertRaises(Unmet), patch.object(module, "evaluate_ob3_originals") as oracle:
            driver.external("FW14")
        oracle.assert_not_called()

    def test_oracle_adapter_uses_supplied_callable_and_never_reopens_code(self):
        raw, expectations = b' {"original":true}\n', b' {"independent":true}\n'
        observed = []
        result = {"scope": "adapter-control-only"}
        def verified_callable(receipt_bytes, expectation_bytes):
            observed.append((receipt_bytes is raw, expectation_bytes is expectations))
            return result
        with patch("builtins.open", side_effect=AssertionError("no code file reopen")), \
             patch.object(Path, "read_bytes", side_effect=AssertionError("no source reread")), \
             patch.object(importlib.util, "spec_from_file_location", side_effect=AssertionError("no path or pyc loader")):
            self.assertIs(module.evaluate_ob3_originals(verified_callable, raw, expectations), result)
            for missing in (None, "oracle.py", b"unverified source", object()):
                with self.subTest(missing=type(missing).__name__), self.assertRaises(Unmet):
                    module.evaluate_ob3_originals(missing, raw, expectations)
        self.assertEqual(observed, [(True, True)])
        self.assertEqual(self.client.count, 0)
        # This tests only the adapter. The enclosing bootstrap separately proves
        # that the supplied callable came from its original verified source bytes.

    async def test_missing_consumers_do_not_invent_receipts_or_hide_other_receipt_case(self):
        driver = self.driver()
        for identifier in module.CASES[:12]:
            setattr(driver, identifier.lower(), AsyncMock(return_value=None))
        observed = await driver.run()
        self.assertEqual([row["id"] for row in observed["cases"]], list(module.CASES))
        self.assertEqual([row["status"] for row in observed["cases"][-2:]], ["unmet", "unmet"])
        self.assertFalse(observed["qualified"])

    async def test_actual_outage_uses_owner_stop_and_observed_transport_failure(self):
        driver = self.driver()
        driver.service_control = type("OwnedService", (), {"stop_original": AsyncMock(return_value={"owned_original_stop": True})})()
        self.client.responses = [{"status": None, "framing_complete": False, "error_type": "ConnectionRefusedError"}]
        result = await driver.fw14()
        driver.service_control.stop_original.assert_awaited_once_with("elephant-db", "original-run")
        self.assertEqual(self.client.count, 1)
        self.assertEqual(result["status"], "unmet")
        self.assertTrue(any(row["kind"] == "actual-owned-service-outage" for row in driver.events))

    async def test_same_named_principal_mcp_head_positive_and_revoked_typed_denial(self):
        driver = self.driver()
        wanted = operation()
        self.client.responses = [{"body": {"jsonrpc": "2.0", "id": 1, "result": {"content": [{"type": "text", "text": encode({"ok": True, "data": wanted}).decode()}]}}},
            {"body": {"jsonrpc": "2.0", "id": 2, "result": {"isError": True, "content": [{"type": "text", "text": encode({"error_code": "forbidden_space", "message": "No active service grant", "retryable": False}).decode()}]}}}]
        self.assertEqual(await driver.mcp_head(SPACE, "separate-data", "original"), wanted)
        denied = await driver.mcp_head(SPACE, "separate-data", "original", denied=True)
        self.assertEqual(denied["error_code"], "forbidden_space")
        self.assertEqual([row[4] for row in self.client.requests], ["separate-data", "separate-data"])
        self.assertEqual(self.client.count, 2)
        self.assertTrue(all(row[1] == "/mcp/full" and row[5]["response_limit"] == 262144 for row in self.client.requests))

    async def test_mcp_success_transport_is_not_a_revocation_denial(self):
        driver = self.driver()
        for result in ({"isError": False, "content": [{"type": "text", "text": encode({"error_code": "forbidden_space", "message": "denied", "retryable": False}).decode()}]},
                       {"isError": True, "content": [{"type": "text", "text": encode({"error_code": "not_ready", "message": "pending", "retryable": True}).decode()}]}):
            self.client.responses = [{"body": {"jsonrpc": "2.0", "id": self.client.count + 1, "result": result}}]
            with self.subTest(result=result), self.assertRaises(Unmet):
                await driver.mcp_head(SPACE, "separate-data", "original", denied=True)

    def test_literal_two_space_cases_and_observation_bounds_are_not_optional(self):
        driver = self.driver()
        driver.plan["cases"]["FW09"] = {"spaces": [{"space": module.SPACES[0]}, {"space": module.SPACES[1]}]}
        self.assertEqual(len(driver.space_cases("FW09")), 2)
        driver.plan["cases"]["FW09"]["spaces"][0]["observe_seconds"] = 121
        with self.assertRaises(Unmet):
            driver.space_cases("FW09")
        driver.plan["cases"]["FW09"]["spaces"] = [{"space": module.SPACES[0]}]
        with self.assertRaises(Unmet):
            driver.space_cases("FW09")

    def test_incomplete_independent_case_plan_refuses_before_any_product_request(self):
        plan = {"configurations": {}, "baseline_rendered": [], "large_rendered": {},
                "cases": {key: {} for key in module.CASES}, "graph_recipes": {}}
        with self.assertRaises(Unmet):
            module.validate_case_inputs(plan)
        self.assertEqual(self.client.count, 0)
        self.assertEqual(self.spy.count, 0)

    def test_capture_event_reservation_exhaustion_writes_no_overbudget_intent(self):
        driver = self.driver()
        self.capture.maximum = 1
        with self.assertRaises(Unmet):
            driver.event("before-wire", {"value": "bounded"})
        self.assertEqual(list(self.capture.directory.iterdir()), [])
        self.assertEqual(self.client.count, 0)

    def test_cli_inventory_is_inert_and_cannot_authorize_execution(self):
        with self.assertRaisesRegex(ValueError, "original code snapshot has not been loaded"):
            CLI.inventory()
        originals, hashes = CLI.snapshot_sources()
        with CLI.reviewed_code(originals, hashes) as verified_cli:
            value = verified_cli.inventory()
        self.assertEqual(value["source_sha256"], hashes)
        self.assertFalse(value["execution_authorized"])
        self.assertFalse(value["external_provider_dispatch_authorized"])
        self.assertFalse(value["product_acceptance"])
        self.assertEqual(value["cases"], list(module.CASES))
        self.assertIn("scripts/qualification/ob3_outage_oracle.py", value["source_sha256"])

    def test_cli_original_source_input_hash_and_private_token_custody(self):
        root = self.capture.directory.parent
        path = root / "input.json"
        raw = b'{"original":true}\n'
        path.write_bytes(raw)
        path.chmod(0o600)
        originals, hashes = CLI.snapshot_sources()
        with CLI.reviewed_code(originals, hashes) as verified_cli:
            # This is the exact fresh exception class used by the verified
            # functional CLI, not an ambient cached transport class.
            verified_unmet = sys.modules["full_workload_transport"].Unmet
            self.assertEqual(verified_cli.Inputs().read({"path": str(path), "sha256": sha(raw)}), {"original": True})
            verified_cli.regular_private(path)
            with self.assertRaisesRegex(verified_unmet, "^original input hash differs$"):
                verified_cli.Inputs().read({"path": str(path), "sha256": sha(b'{"original":false}\n')})
            path.chmod(0o644)
            with self.assertRaisesRegex(verified_unmet, "^task-owned 0600 credential file required$"):
                verified_cli.regular_private(path)
            link = root / "link.json"
            link.symlink_to(path)
            with self.assertRaisesRegex(verified_unmet, "^original canonical input path required$"):
                verified_cli.Inputs().read({"path": str(link), "sha256": sha(raw)})


if __name__ == "__main__":
    unittest.main()
