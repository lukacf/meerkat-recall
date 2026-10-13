"""Closed-world 140-caller scheduling controls; no installed latency result."""
import asyncio
import copy
import time
import unittest
from unittest.mock import patch

from full_workload_load import run_load, successful_response, validate_plan, validate_requirements, wire_peak
from full_workload_transport import Unmet, encode, require, sha

QUOTE = "Mira maintains Helios."


def requirements(original):
    literal = {"source_key": "original-source", "revision": "original-r1", "quote": QUOTE,
               "quote_hash": sha(QUOTE.encode())}
    return {row["id"]: {"required_evidence": [copy.deepcopy(literal)],
                       "required_claim_evidence": [copy.deepcopy(literal)]}
            for row in original["requests"]}


def verify_evidence(space, request, response, required, required_claim):
    """Test callback only; the actual driver provides its source-bound verifier."""
    require(space in ("qualification-household", "qualification-business"), "foreign space")
    found, claims = [], []
    for result in response["results"]:
        require(result["evidence"], "missing source evidence")
        for evidence in result["evidence"]:
            require(request["source_revision_constraints"].get(evidence["source_id"]) == evidence["revision"],
                    "foreign evidence")
            require(evidence["quote"] == QUOTE, "changed original evidence")
            value = {"source_key": evidence["source_id"], "revision": evidence["revision"],
                     "quote": evidence["quote"], "quote_hash": sha(evidence["quote"].encode())}
            found.append(value)
            if result["kind"] == "claim":
                claims.append(value)
    require(all(value in found for value in required) and all(value in claims for value in required_claim),
            "required original evidence absent")


async def run(client, spy, original, callback=verify_evidence):
    expected = requirements(original)
    return await run_load(client, spy, original, sha(encode(original)),
                          evidence_requirements=expected, evidence_sha256=sha(encode(expected)),
                          validate_evidence=callback)


def plan():
    return {"format": "recall-full-workload-lexical-load/v1", "requests": [
        {"id": f"request-{caller}-{ordinal}", "caller": caller, "ordinal": ordinal,
         "space": "qualification-household" if caller < 70 else "qualification-business",
         "body": {"query": "original reviewed Helios query", "ranking_mode": "lexical", "limit": 20,
                  "source_revision_constraints": {"original-source": "original-r1"}},
         "expected_coverage": {"original-source": {"requested_revision": "original-r1", "published_revision": "original-r1",
                                "desired_revision": "original-r1", "state": "complete"}}}
        for caller in range(140) for ordinal in (1, 2)]}


class Spy:
    def __init__(self):
        self.count = 0

    def snapshot_calls(self):
        return {"physical_connections": self.count, "active": 0, "held_ids": [], "error_type": None}


class Client:
    def __init__(self, spy):
        self.count, self.calls, self.spy = 0, [], spy
        self.deadline, self.clock = time.monotonic() + 20, time.monotonic
        self.limits = {"public_http_calls_per_backend": 8000}
        self.started, self.first_wave = 0, asyncio.Event()
        self.fail, self.provider_send = False, False
        self.evidence_failure = None
        self.coverage_failure = None

    async def request(self, method, path, space, body, *, role, response_limit):
        self.count += 1
        identifier = self.count
        self.calls.append((method, path, space, copy.deepcopy(body), role, response_limit))
        started = time.monotonic_ns()
        if identifier <= 140:
            self.started += 1
            if self.started == 140:
                self.first_wave.set()
            await self.first_wave.wait()
        await asyncio.sleep(0)
        if self.provider_send and identifier == 1:
            self.spy.count += 1
        coverage = plan()["requests"][0]["expected_coverage"]
        if self.coverage_failure in ("extra-same-space", "extra-foreign"):
            key = "unrequested-source" if self.coverage_failure == "extra-same-space" else "foreign-space-source"
            coverage[key] = copy.deepcopy(coverage["original-source"])
        elif self.coverage_failure == "missing":
            coverage.clear()
        elif self.coverage_failure == "changed":
            coverage["original-source"]["published_revision"] = "unrequested-r2"
        results = [{"kind": "claim", "text": "Independent literal claim", "score": 1.0,
                    "evidence": [{"source_id": "original-source", "revision": "original-r1", "quote": QUOTE}]}]
        if self.evidence_failure == "empty":
            results = []
        elif self.evidence_failure == "foreign":
            results[0]["evidence"][0]["source_id"] = "foreign-source"
        elif self.evidence_failure == "extra-foreign":
            extra = copy.deepcopy(results[0])
            extra["evidence"][0]["source_id"] = "foreign-source"
            results.append(extra)
        elif self.evidence_failure == "quote":
            results[0]["evidence"][0]["quote"] = "Invented quote."
        elif self.evidence_failure == "missing-claim":
            results[0]["kind"] = "chunk"
        return {"status": 503 if self.fail else 200, "framing_complete": True,
                "client_write_complete_ns": started, "ended_ns": time.monotonic_ns(),
                "body": {"results": results, "coverage": coverage, "partial": False}}


class LoadTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        for name in ("asyncio.start_server", "asyncio.open_connection", "subprocess.Popen"):
            guard = patch(name, side_effect=AssertionError("offline controls cannot execute runtime"))
            guard.start()
            self.addCleanup(guard.stop)

    async def test_140_ready_callers_each_send_two_original_requests_once(self):
        spy = Spy()
        client = Client(spy)
        original = plan()
        calls = []
        def verify(*args):
            calls.append(copy.deepcopy(args))
            verify_evidence(*args)
        observed = await run(client, spy, original, verify)
        self.assertEqual(observed["protocol_verdict"], "passed")
        self.assertEqual(observed["ready_callers"], list(range(140)))
        self.assertEqual(observed["physical_http_calls"], 280)
        self.assertEqual(observed["peak_client_wire_intervals"], 140)
        self.assertEqual(len(observed["requests"]), 280)
        self.assertEqual(observed["unexecuted"], [])
        self.assertFalse(observed["qualified"])
        self.assertTrue(all(row["evidence_verified"] for row in observed["requests"]))
        self.assertEqual(observed["evidence_sha256"], sha(encode(requirements(original))))
        self.assertEqual(len(calls), 280)
        self.assertEqual(sum(args[0] == "qualification-household" for args in calls), 140)
        for space, request, response, required, required_claim in calls:
            self.assertEqual(required, required_claim)
            self.assertEqual(required[0]["quote"], QUOTE)
            self.assertEqual(request["query"], "original reviewed Helios query")
            self.assertEqual(response["results"][0]["evidence"][0]["quote"], QUOTE)
        self.assertTrue(observed["semantic_load"].startswith("unmet:"))
        for caller in range(140):
            rows = [row for row in observed["requests"] if row["caller"] == caller]
            self.assertEqual([row["ordinal"] for row in rows], [1, 2])
            self.assertLessEqual(rows[0]["observed"]["ended_ns"], rows[1]["observed"]["client_write_complete_ns"])
        self.assertTrue(all(row[3]["ranking_mode"] == "lexical" and row[5] == 262144 for row in client.calls))

    async def test_failed_first_wave_preserves_actual_failures_and_skips_second(self):
        spy = Spy()
        client = Client(spy)
        client.fail = True
        original = plan()
        def forbidden(*args):
            self.fail("protocol failures must not invoke the evidence callback")
        observed = await run(client, spy, original, forbidden)
        self.assertEqual(observed["protocol_verdict"], "unmet")
        self.assertEqual(observed["physical_http_calls"], 140)
        self.assertEqual(len(observed["unexecuted"]), 140)
        self.assertTrue(all(row["observed"]["status"] == 503 for row in observed["requests"]))

    async def test_provider_send_during_lexical_load_prevents_positive_protocol_verdict(self):
        spy = Spy()
        client = Client(spy)
        client.provider_send = True
        original = plan()
        observed = await run(client, spy, original)
        self.assertEqual(observed["protocol_verdict"], "unmet")
        self.assertFalse(observed["provider_count_unchanged"])
        self.assertEqual(observed["physical_http_calls"], 280)

    async def test_coverage_only_extra_keys_missing_or_changed_rows_refuse(self):
        for failure in ("extra-same-space", "extra-foreign", "missing", "changed"):
            spy = Spy()
            client = Client(spy)
            client.coverage_failure = failure
            def forbidden(*args):
                self.fail("coverage mismatch cannot reach evidence verification")
            observed = await run(client, spy, plan(), forbidden)
            with self.subTest(failure=failure):
                self.assertEqual(observed["protocol_verdict"], "unmet")
                self.assertEqual(observed["physical_http_calls"], 140)
                self.assertEqual(len(observed["unexecuted"]), 140)
                self.assertEqual(len(observed["requests"]), 140)
                self.assertEqual(observed["errors"], [])
                self.assertTrue(all(not row["protocol_complete"] and not row["evidence_verified"]
                                    for row in observed["requests"]))
                for row in observed["requests"]:
                    response = row["observed"]
                    self.assertEqual(response["status"], 200)
                    self.assertEqual(response["body"]["results"][0]["evidence"][0]["source_id"], "original-source")
                    if failure.startswith("extra-"):
                        self.assertEqual(len(response["body"]["coverage"]), 2,
                                         "retain original coverage-only leak in actual failed capture")

    def test_exact_coverage_is_independent_of_object_key_order(self):
        row = plan()["requests"][0]["expected_coverage"]["original-source"]
        expected = {"first": copy.deepcopy(row), "second": copy.deepcopy(row)}
        observed = {"status": 200, "framing_complete": True,
                    "body": {"results": [], "partial": False,
                             "coverage": {"second": copy.deepcopy(row), "first": copy.deepcopy(row)}}}
        self.assertTrue(successful_response(observed, expected))

    async def test_original_expiry_and_http_budget_refuse_before_scheduling(self):
        for failure in ("expired", "budget"):
            spy = Spy()
            client = Client(spy)
            if failure == "expired":
                client.deadline = client.clock() - 1
            else:
                client.count = 7900
            before = client.count
            original = plan()
            with self.subTest(failure=failure), self.assertRaises(Unmet):
                await run(client, spy, original)
            self.assertEqual(client.count, before)
            self.assertEqual(client.calls, [])

    def test_wrong_count_semantic_or_reassigned_schedule_refuses(self):
        for failure in ("count", "semantic", "ordinal", "coverage", "hash"):
            value = plan()
            expected = sha(encode(value))
            if failure == "count":
                value["requests"].pop()
            elif failure == "semantic":
                value["requests"][0]["body"]["ranking_mode"] = "semantic"
            elif failure == "ordinal":
                value["requests"][1]["ordinal"] = 1
            elif failure == "coverage":
                value["requests"][0]["expected_coverage"]["original-source"]["published_revision"] = "substituted-r2"
            if failure != "hash":
                expected = sha(encode(value))
            else:
                expected = "0" * 64
            with self.subTest(failure=failure), self.assertRaises(Unmet):
                validate_plan(value, expected)

    def test_touching_or_zero_intervals_are_not_overlapping_wire_work(self):
        rows = [{"client_write_complete_ns": 1, "ended_ns": 3},
                {"client_write_complete_ns": 3, "ended_ns": 5},
                {"client_write_complete_ns": 3, "ended_ns": 3}, {}]
        self.assertEqual(wire_peak(rows), 1)

    async def test_empty_foreign_or_changed_evidence_retains_response_and_refuses_success(self):
        for failure in ("empty", "foreign", "extra-foreign", "quote", "missing-claim"):
            spy = Spy()
            client = Client(spy)
            client.evidence_failure = failure
            observed = await run(client, spy, plan())
            with self.subTest(failure=failure):
                self.assertEqual(observed["protocol_verdict"], "unmet")
                self.assertEqual(observed["physical_http_calls"], 140)
                self.assertEqual(len(observed["unexecuted"]), 140)
                self.assertEqual(len(observed["requests"]), 140)
                self.assertEqual(len(observed["errors"]), 140)
                self.assertTrue(all(row["protocol_complete"] and not row["evidence_verified"]
                                    for row in observed["requests"]))
                self.assertTrue(all(row["observed"]["status"] == 200 for row in observed["requests"]))

    async def test_callback_refusal_or_missing_verifier_cannot_pass(self):
        spy = Spy()
        observed = await run(Client(spy), spy, plan(), lambda *args: False)
        self.assertEqual(observed["protocol_verdict"], "unmet")
        self.assertTrue(all(not row["evidence_verified"] for row in observed["requests"]))
        client = Client(Spy())
        with self.assertRaises(Unmet):
            await run(client, client.spy, plan(), None)
        self.assertEqual(client.count, 0)

    def test_all_positive_expectations_are_pinned_and_belong_to_original_request(self):
        original = plan()
        for failure in ("missing", "empty", "foreign", "hash", "quote-hash", "unbound-claim"):
            expected = requirements(original)
            key = original["requests"][0]["id"]
            if failure == "missing":
                del expected[key]
            elif failure == "empty":
                expected[key]["required_evidence"] = []
            elif failure == "foreign":
                expected[key]["required_evidence"][0]["source_key"] = "foreign"
            elif failure == "quote-hash":
                expected[key]["required_evidence"][0]["quote_hash"] = "0" * 64
            elif failure == "unbound-claim":
                expected[key]["required_claim_evidence"][0]["quote"] = "Another literal."
                expected[key]["required_claim_evidence"][0]["quote_hash"] = sha(b"Another literal.")
            digest = "0" * 64 if failure == "hash" else sha(encode(expected))
            with self.subTest(failure=failure), self.assertRaises(Unmet):
                validate_requirements(original, expected, digest)


if __name__ == "__main__":
    unittest.main()
