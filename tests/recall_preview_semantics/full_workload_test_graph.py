"""Scripted public-wire controls, not actual installed Graph qualification."""
import copy
import time
import unittest
from unittest.mock import patch

from full_workload_graph import GraphFixture, validate_recipe
from full_workload_transport import Unmet, encode, sha


SPACE = "qualification-household"
SOURCE = "source:11111111-1111-4111-8111-111111111111"
DOC = "doc:22222222-2222-4222-8222-222222222222"
REVISION = "doc_revision:33333333-3333-4333-8333-333333333333"
SUBJECT = "entity:44444444-4444-4444-8444-444444444444"
OBJECT = "entity:55555555-5555-4555-8555-555555555555"
RAW = "Mira maintains Helios."
SECURITY = {"policy_id": "household", "level": 3, "labels": [], "handling": []}
ENVELOPE = {"space_id": SPACE, "security": SECURITY, "subjects": [], "subjects_state": "unknown", "deleted_at": None}


def recipe():
    return {"format": "recall-public-graph-fixture/v1", "source_request": {
        "source": {**ENVELOPE, "id": SOURCE, "type": "manual", "hash": sha(RAW.encode()), "external_id": "full/shared/a"},
        "doc": {**ENVELOPE, "id": DOC, "source": None, "title": "Original", "text": RAW},
        "ingest_mode": "graph", "intake": {"revision_id": "original-r1", "idempotency_key": "original-key"}},
        "rendered_text": RAW + "\n", "rendered_sha256": sha((RAW + "\n").encode()), "extract_rule_id": "original-extract",
        "expected_assertions": [{"subject_text": "Mira", "predicate_raw": "maintains", "object_text": "Helios",
            "object_literal": None, "subject_entity_type": "person", "object_entity_type": "document",
            "confidence": 1.0, "significance": "medium", "source_event_time": None,
            "start": 0, "end": len(RAW), "quote": RAW, "quote_hash": sha(RAW.encode())}]}


def accepted():
    return {"source_id": SOURCE, "doc_id": DOC, "work_item_id": "work_item:original",
            "operation": {"operation_id": "operation:original", "source_id": SOURCE, "source_generation": "generation-1",
                          "space_id": SPACE, "desired_revision": "original-r1"}}


def operation(state="complete"):
    return {"operation_id": "operation:original", "internal_source_id": SOURCE, "generation": "generation-1",
            "desired_revision": "original-r1", "published_revision": "original-r1" if state == "complete" else None,
            "lifecycle": "active", "state": state, "error_code": None}


def assertion():
    row = {key: value for key, value in recipe()["expected_assertions"][0].items()
           if key not in {"start", "end", "quote", "quote_hash"}}
    return {**row, **ENVELOPE, "id": "assertion:original", "provenance": {"sources": [SOURCE]},
            "resolution_status": "resolved", "subject_entity": SUBJECT, "object_entity": OBJECT,
            "evidence": {"revision": REVISION, "start": 0, "end": len(RAW), "quote_hash": sha(RAW.encode())}}


class Spy:
    def __init__(self):
        self.recipe = {"rules": [{"id": "original-extract", "space": SPACE, "mode": "structured",
                                  "required_text": [RAW], "hold_calls": [1]}]}
        self.rule_counts, self.failure, self.released = {}, None, []
        self.calls = []

    def snapshot_calls(self):
        return {"physical_connections": len(self.calls), "complete_calls": copy.deepcopy(self.calls), "error_type": self.failure}

    def complete(self, rule_id="original-extract"):
        self.calls.append({"rule_id": rule_id, "rule_ordinal": 1, "response_complete": True, "status": 200})
        self.rule_counts[rule_id] = 1

    async def wait_held(self, rule_id):
        if rule_id != "original-extract":
            raise AssertionError("foreign rule")
        return 100001

    def release(self, identifier):
        self.released.append(identifier)


class Client:
    def __init__(self, spy):
        self.spy, self.calls = spy, []
        self.deadline, self.clock = time.monotonic() + 10, time.monotonic
        self.corrupt = lambda path, value: value
        self.extraction_rule = "original-extract"

    async def request(self, method, path, space, body=None, *, role):
        self.calls.append((method, path, space, copy.deepcopy(body), role))
        if path == "/v1/sources/ingest":
            self.spy.complete(self.extraction_rule)
            value = accepted()
        elif path.startswith("/v1/operations/") or path.endswith("/head"):
            value = operation()
        elif path.endswith("/pipeline-status"):
            value = {"doc_id": DOC, "revision": REVISION}
        elif "/revisions/" in path:
            value = {**ENVELOPE, "id": REVISION, "doc": DOC, "text": RAW + "\n",
                     "text_hash": sha((RAW + "\n").encode()), "status": "active",
                     "renderer": {"name": "controlled-default-renderer", "version": "1"},
                     "provenance": {"sources": [SOURCE], "chain": [
                         {"step": "source_content", "input": SOURCE, "output": sha(RAW.encode())}]}}
        elif path.startswith("/v1/docs/"):
            value = {**ENVELOPE, "id": DOC, "source": SOURCE, "text": RAW + "\n"}
        elif path.startswith("/v1/sources/"):
            value = {**recipe()["source_request"]["source"], "current_ingest_mode": "graph", "lifecycle_status": "graph_ready"}
        elif path == "/v1/assertions/search":
            value = {"results": [assertion()], "cursor": None, "has_more": False, "total": 1}
        elif path.startswith("/v1/entities/"):
            subject = path.endswith(SUBJECT.replace(":", "%3A"))
            value = {**ENVELOPE, "id": SUBJECT if subject else OBJECT, "display_name": "Mira" if subject else "Helios",
                     "entity_type": "person" if subject else "document", "provenance": {"sources": [SOURCE]}}
        else:
            raise AssertionError("unexpected public route " + path)
        return {"framing_complete": True, "status": 200, "body": self.corrupt(path, value)}


class GraphTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        for name in ("asyncio.start_server", "asyncio.open_connection", "subprocess.Popen"):
            guard = patch(name, side_effect=AssertionError("scripted controls cannot execute runtime"))
            guard.start()
            self.addCleanup(guard.stop)

    def fixture(self):
        spy = Spy()
        client = Client(spy)
        value = recipe()
        return GraphFixture(client, spy, value, sha(encode(value)))

    async def test_original_public_graph_observations_and_normalized_bytes(self):
        fixture = self.fixture()
        observed = await fixture.accept_and_wait()
        self.assertEqual(observed["doc"]["text"], RAW + "\n")
        self.assertEqual(observed["source"]["hash"], sha(RAW.encode()))
        self.assertEqual(len(observed["entities"]), 2)
        self.assertEqual(observed["pipeline"]["revision"], REVISION)
        self.assertEqual(observed["immutable_revision"]["id"], REVISION)
        self.assertEqual(observed["immutable_revision"]["text_hash"], sha((RAW + "\n").encode()))
        self.assertFalse(observed["model_quality_qualified"])
        self.assertTrue(observed["private_manifest"].startswith("unmet:"))
        self.assertEqual(sum(row[1] == "/v1/sources/ingest" for row in fixture.client.calls), 1)

    async def test_raw_equal_doc_or_foreign_revision_does_not_pass(self):
        for kind in ("raw-document", "revision", "partial-page", "entity-policy", "physical-source"):
            fixture = self.fixture()
            def corrupt(path, value):
                if kind == "raw-document" and path.startswith("/v1/docs/") and not path.endswith("pipeline-status"):
                    value["text"] = RAW
                if path == "/v1/assertions/search":
                    if kind == "revision":
                        value["results"][0]["evidence"]["revision"] = "doc_revision:foreign"
                    if kind == "partial-page":
                        value["has_more"] = True
                    if kind == "physical-source":
                        value["results"][0]["provenance"]["sources"].append("source:unrelated")
                if kind == "entity-policy" and path.startswith("/v1/entities/"):
                    value["security"] = {**SECURITY, "policy_id": "unrelated"}
                return value
            fixture.client.corrupt = corrupt
            with self.subTest(kind=kind), self.assertRaises(Unmet):
                await fixture.accept_and_wait()

    async def test_valid_unrelated_provider_call_cannot_qualify_original_graph(self):
        fixture = self.fixture()
        fixture.client.extraction_rule = "unrelated-corpus"
        with self.assertRaises(Unmet):
            await fixture.accept_and_wait()

    async def test_actual_immutable_revision_not_reconstructed_from_valid_document(self):
        for kind in ("text", "text_hash", "doc", "space_id", "security", "source-binding"):
            fixture = self.fixture()
            def corrupt(path, value):
                if "/revisions/" in path:
                    if kind == "security":
                        value[kind] = {**SECURITY, "policy_id": "foreign"}
                    elif kind == "source-binding":
                        value["provenance"]["chain"][0]["output"] = "0" * 64
                    else:
                        value[kind] = "different-original-" + kind
                return value
            fixture.client.corrupt = corrupt
            with self.subTest(kind=kind), self.assertRaises(Unmet):
                await fixture.accept_and_wait()
            self.assertTrue(any("/revisions/" in row[1] for row in fixture.client.calls))

    async def test_generation_change_and_terminal_failure_are_not_retried(self):
        for state in ("new-generation", "failed_final"):
            fixture = self.fixture()
            def corrupt(path, value):
                if path.startswith("/v1/operations/"):
                    value["generation" if state == "new-generation" else "state"] = state
                return value
            fixture.client.corrupt = corrupt
            with self.subTest(state=state), self.assertRaises(Unmet):
                await fixture.accept_and_wait()
            with self.assertRaises(Unmet):
                await fixture.accept()
            self.assertEqual(sum(row[1] == "/v1/sources/ingest" for row in fixture.client.calls), 1)

    async def test_held_release_requires_same_accepted_operation_and_physical_call(self):
        fixture = self.fixture()
        held = await fixture.hold_next_exact_source()
        with self.assertRaises(Unmet):
            fixture.release_original({**held, "physical_call_id": 100002})
        fixture.release_original(held)
        self.assertEqual(fixture.spy.released, [100001])
        with self.assertRaises(Unmet):
            fixture.release_original(held)

    async def test_original_ambiguous_http_failure_cannot_be_resubmitted(self):
        fixture = self.fixture()
        async def failed(*args, **kwargs):
            return {"status": None, "framing_complete": False, "error_type": "TimeoutError"}
        fixture.client.request = failed
        with self.assertRaises(Unmet):
            await fixture.accept()
        with self.assertRaises(Unmet):
            await fixture.accept()

    def test_review_hash_and_evidence_span_are_independent_prerequisites(self):
        value = recipe()
        with self.assertRaises(Unmet):
            validate_recipe(value, "0" * 64)
        value["expected_assertions"][0]["end"] += 1
        with self.assertRaises(Unmet):
            validate_recipe(value, sha(encode(value)))


if __name__ == "__main__":
    unittest.main()
