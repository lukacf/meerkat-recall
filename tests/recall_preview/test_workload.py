"""Evaluator fixtures only; these do not execute a service or measured workload."""
import base64
import copy
import json
import unittest

import oracle
import workload


def captured(body):
    raw = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode()
    return {"status": 200, "framing_complete": True, "raw_body_complete": True,
            "body": body, "raw_body_base64": base64.b64encode(raw).decode(),
            "bytes": len(raw), "body_sha256": oracle.sha256(raw)}


def response_for(query, *, distractors=0):
    frozen = workload.corpus()
    target = next(source for source in frozen["sources"] if source["space"] == query["space"]
                  and source["key"] == query["required"][0]["key"])
    others = [source for source in frozen["sources"] if source["space"] == query["space"] and source != target]
    sources = others[:distractors] + [target]
    body = {"partial": False, "results": [], "coverage": {}}
    for source in sources:
        body["coverage"][source["key"]] = {"requested_revision": source["revision"],
            "published_revision": source["revision"], "desired_revision": source["revision"], "state": "complete"}
        body["results"].append({"kind": "chunk", "text": source["text"], "evidence": [{
            "source_id": source["key"], "revision": source["revision"], "quote": source["text"],
            "span": {"start": 0, "end": len(source["text"].encode())}}]})
    return captured(body)


def cohort():
    frozen, chosen = workload.corpus(), workload.contract()
    queries = {query["id"]: query for query in frozen["queries"]}
    rows, callers = [], {}
    for index, scheduled in enumerate(frozen["schedule"]):
        query = queries[scheduled["query_id"]]
        end = (index + 1) * 10
        rows.append(dict(scheduled, started_ms=index * 10, ended_ms=end,
                         request_body=workload.request(query), response=response_for(query)))
        callers[scheduled["caller_id"]] = {"id": scheduled["caller_id"], "ready_ms": 0, "finished_ms": end + 1}
    return {"measured": True, "backend": "elephant-db", "workload_contract_sha256": workload.canonical_hash(chosen),
            "corpus_sha256": chosen["corpus_sha256"], "sources_per_space": chosen["sources_per_space"],
            "barrier_released_ms": 0, "callers": list(callers.values()), "requests": rows, "peak_in_flight": 1}


class WorkloadContractTests(unittest.TestCase):
    def test_separate_exact_fifty_source_corpus_and_two_hundred_schedule(self):
        frozen, chosen = workload.corpus(), workload.contract()
        self.assertEqual(len(frozen["sources"]), 50)
        self.assertEqual(len(frozen["queries"]), 100)
        self.assertEqual(len(frozen["schedule"]), 200)
        self.assertEqual({space: sum(source["space"] == space for source in frozen["sources"])
                          for space in chosen["spaces"]}, chosen["sources_per_space"])
        self.assertEqual(set(chosen["sources_per_space"].values()), {25})
        original = workload.CORPUS.with_name("corpus.json").read_bytes()
        self.assertEqual(oracle.sha256(original), frozen["primary_corpus_sha256_unchanged"])
        self.assertEqual(oracle.sha256(workload.CORPUS.read_bytes()), chosen["corpus_sha256"])
        self.assertEqual(workload.canonical_hash(frozen["schedule"]), chosen["schedule_sha256"])
        self.assertIs(chosen["execution_authorized"], False)

    def test_wire_limit_fifty_keeps_metric_denominator_three(self):
        query = workload.corpus()["queries"][0]
        response = response_for(query, distractors=1)
        scores = workload.workload_response(query, response)
        self.assertEqual(scores["reciprocal_rank"], .5)
        self.assertEqual(scores["precision_at_k"], 1 / 3)
        self.assertEqual(workload.request(query)["limit"], 50)
        self.assertEqual(workload.request(query)["ranking_mode"], "semantic")
        self.assertIsNone(scores["physical_provider_attempts"])

    def test_evidence_below_first_three_sources_cannot_rescue_rank(self):
        query = workload.corpus()["queries"][0]
        with self.assertRaisesRegex(oracle.Rejected, "top three"):
            workload.workload_response(query, response_for(query, distractors=3))

    def test_duplicate_source_hits_never_inflate_precision(self):
        query = workload.corpus()["queries"][0]
        body = response_for(query)["body"]
        body["results"] *= 50
        result = workload.workload_response(query, captured(body))
        self.assertEqual(result["wire_results"], 50)
        self.assertEqual(result["distinct_sources"], 1)
        self.assertEqual(result["precision_at_k"], 1 / 3)
        body["results"].append(body["results"][0])
        with self.assertRaises(oracle.Rejected):
            workload.workload_response(query, captured(body))

    def test_all_results_checked_even_after_metric_cutoff(self):
        query = workload.corpus()["queries"][0]
        body = response_for(query)["body"]
        body["results"] *= 3
        wrong = copy.deepcopy(body["results"][0])
        wrong["evidence"][0]["revision"] = "workload-obsolete-r0"
        body["results"].append(wrong)
        with self.assertRaises(oracle.Rejected):
            workload.workload_response(query, captured(body))

    def test_foreign_space_same_key_revision_cannot_impersonate_exact_quote(self):
        frozen = workload.corpus()
        query = frozen["queries"][0]
        foreign = next(source for source in frozen["sources"] if source["space"] != query["space"]
                       and source["key"] == query["required"][0]["key"])
        body = response_for(query)["body"]
        evidence = body["results"][0]["evidence"][0]
        evidence["quote"] = foreign["text"]
        evidence["span"]["end"] = len(foreign["text"].encode())
        with self.assertRaises(oracle.Rejected):
            workload.workload_response(query, captured(body))

    def test_correct_source_with_quote_omitting_answer_is_rejected(self):
        query = workload.corpus()["queries"][0]
        body = response_for(query)["body"]
        evidence = body["results"][0]["evidence"][0]
        evidence["quote"] = "For household"
        evidence["span"]["end"] = len(evidence["quote"].encode())
        with self.assertRaisesRegex(oracle.Rejected, "answer"):
            workload.workload_response(query, captured(body))

    def test_raw_capture_digest_framing_stale_and_wrong_query_are_failures(self):
        query = workload.corpus()["queries"][0]
        response = response_for(query)
        for change in ({"raw_body_complete": False}, {"body_sha256": "0" * 64}, {"status": 413},
                       {"bytes": 262145}, {"body": {}}):
            with self.subTest(change=change), self.assertRaises((oracle.Rejected, ValueError)):
                workload.workload_response(query, response | change)
        with self.assertRaises(oracle.Rejected):
            workload.workload_response(query | {"query": "repaired query"}, response)
        body = copy.deepcopy(response["body"])
        body["coverage"][query["required"][0]["key"]]["desired_revision"] = "new-pending"
        with self.assertRaises(oracle.Rejected):
            workload.workload_response(query, captured(body))

    def test_fifty_logical_callers_do_not_assert_fifty_physical_requests(self):
        result = workload.evaluate_cohort(cohort())
        self.assertEqual(result["concurrent_callers"], 50)
        self.assertEqual(result["measured_peak_in_flight"], 1)
        self.assertEqual(result["calls"], 200)
        self.assertIs(result["ob3_consumer_fallback_qualified"], False)

    def test_missing_barrier_duplicate_request_retry_or_fallback_cannot_pass(self):
        value = cohort()
        mutations = [lambda v: v["callers"][0].update(ready_ms=1),
                     lambda v: v["requests"].append(copy.deepcopy(v["requests"][0])),
                     lambda v: v["requests"][0]["request_body"].update(ranking_mode="lexical"),
                     lambda v: v["requests"][0].update(ordinal=1),
                     lambda v: v.update(peak_in_flight=50),
                     lambda v: v["sources_per_space"].update({next(iter(v["sources_per_space"])): 50})]
        for mutate in mutations:
            changed = copy.deepcopy(value)
            mutate(changed)
            with self.subTest(mutation=mutate), self.assertRaises(oracle.Rejected):
                workload.evaluate_cohort(changed)

    def test_per_caller_overlap_and_strict_latency_boundary_rejected(self):
        value = cohort()
        value["requests"][1]["started_ms"] = 9
        with self.assertRaises(oracle.Rejected):
            workload.evaluate_cohort(value)
        value = cohort()
        # Keep every caller's sequence valid, but make every request exactly2s.
        for caller in value["callers"]:
            caller["finished_ms"] = 8001
        for row in value["requests"]:
            row["started_ms"] = row["ordinal"] * 2000
            row["ended_ms"] = row["started_ms"] + 2000
        value["peak_in_flight"] = 50
        with self.assertRaisesRegex(oracle.Rejected, "strict.*latency"):
            workload.evaluate_cohort(value)

    def test_latency_success_cannot_hide_low_reciprocal_rank(self):
        value = cohort()
        queries = {query["id"]: query for query in workload.corpus()["queries"]}
        for row in value["requests"]:
            row["response"] = response_for(queries[row["query_id"]], distractors=2)
        with self.assertRaisesRegex(oracle.Rejected, "MRR"):
            workload.evaluate_cohort(value)


if __name__ == "__main__":
    unittest.main()
