"""Synthetic oracle controls only: no service, transport, model or quality run."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import sys
import types
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tests/recall_preview"))
import workload
from test_workload import captured, cohort as original_cohort, response_for
import semantic140_expectations as expected


def cohort(backend="elephant-db"):
    frozen = workload.corpus(expected.PROFILE_ID)
    queries = {row["id"]: row for row in frozen["queries"]}
    responses = {key: response_for(query) for key, query in queries.items()}
    rows, callers = [], {}
    for scheduled in frozen["schedule"]:
        query = queries[scheduled["query_id"]]
        start = 10 + scheduled["ordinal"] * 20
        response = copy.deepcopy(responses[query["id"]])
        row = {**scheduled, "started_ms": start, "connected_ms": start + 1,
               "client_write_complete_ms": start + 2, "first_response_byte_ms": start + 4,
               "ended_ms": start + 10, "request_body": workload.request(query), "response": response,
               "oracle_verdict": "passed", "evaluation_fallbacks": [],
               "independent_response_verdict": workload.workload_response(query, response, frozen)}
        rows.append(row)
        callers[row["caller_id"]] = {"id": row["caller_id"], "ready_ms": 0, "finished_ms": start + 11}
    return {"profile_id": expected.PROFILE_ID, "measured": True, "backend": backend,
            "workload_contract_sha256": expected.CONTRACT_CANONICAL_SHA256,
            "contract_sha256": expected.FROZEN_INPUTS["workload_contract"],
            **{key + "_sha256": value for key, value in expected.FROZEN_INPUTS.items() if key != "workload_contract"},
            "sources_per_space": {"qualification-household": 25, "qualification-business": 25},
            "barrier_released_ms": 1, "callers": list(callers.values()), "requests": rows,
            "peak_in_flight": 140, "peak_sent_waiting_response": 140,
            "product_acceptance": False, "fallback_executed": False, "retries": 0}


class Semantic140OracleControls(unittest.TestCase):
    def rejected(self, value):
        with self.assertRaises((ValueError, KeyError, TypeError)):
            workload.evaluate_cohort(value)

    def test_original_profile_still_accepts_original_200_and_peak1(self):
        original = original_cohort()
        with mock.patch.object(workload, "semantic140_expectations", side_effect=AssertionError("legacy helper load")):
            result = workload.evaluate_cohort(original)
        self.assertEqual((result["calls"], result["concurrent_callers"], result["measured_peak_in_flight"]), (200, 50, 1))

    def test_workload_uses_exact_snapshot_despite_poisoned_cached_module(self):
        poison = types.ModuleType("independent_semantic140_expectations")
        poison.__getattr__ = lambda name: self.fail("cached workload expectations: " + name)
        with mock.patch.dict(sys.modules, independent_semantic140_expectations=poison,
                             semantic140_expectations=poison):
            self.assertEqual(workload.semantic140_expectations().FROZEN_INPUTS, expected.FROZEN_INPUTS)
            self.assertIs(sys.modules["independent_semantic140_expectations"], poison)

    def test_workload_refuses_changed_source_before_execution(self):
        with mock.patch.object(Path, "read_bytes", return_value=b"raise AssertionError('unverified execution')"):
            with self.assertRaisesRegex(ValueError, "expectations source changed"):
                workload.semantic140_expectations()

    def test_new_two_backends_exact560_original100_intents_and_weighting(self):
        for backend in ("elephant-db", "surreal-db"):
            result = workload.evaluate_cohort(cohort(backend))
            self.assertEqual((result["calls"], result["concurrent_callers"]), (560, 140))
            self.assertEqual(result["percentile_ranks_one_based"], {"p95": 532, "p99": 555})
            self.assertEqual(sorted(x["attempts"] for x in result["per_query"].values()), [4]*20 + [6]*80)
            self.assertIsNone(result["server_execution_concurrency"])
            self.assertFalse(result["ob3_consumer_fallback_qualified"])

    def test_missing_unknown_old_or_mixed_profile_refuses_new_rows(self):
        for profile in (None, "d28-v1", "unreviewed"):
            value = cohort()
            if profile is None: value.pop("profile_id")
            else: value["profile_id"] = profile
            self.rejected(value)
        for key in ("corpus_sha256", "contract_sha256", "provider_allocation_sha256",
                    "preparation_contract_sha256", "typed_delta_sha256", "rendered_map_sha256"):
            value = cohort(); value[key] = "0" * 64; self.rejected(value)

    def test_missing_duplicate_and_extra_attempt_cannot_leave_statistics(self):
        for mutation in (lambda rows: rows.pop(), lambda rows: rows.append(copy.deepcopy(rows[0])),
                         lambda rows: rows.__setitem__(1, copy.deepcopy(rows[0]))):
            value = cohort(); mutation(value["requests"]); self.rejected(value)

    def test_exact_queries_and_named_space_assignment_cannot_be_substituted(self):
        for key, substitute in (("query_id", "unknown"), ("space", "qualification-business"), ("ordinal", True)):
            value = cohort(); value["requests"][0][key] = substitute; self.rejected(value)
        value = cohort(); value["requests"][0]["request_body"]["query"] += " repaired"; self.rejected(value)

    def test_each_missing_or_nonfinite_timestamp_refuses(self):
        for field in ("started_ms", "connected_ms", "client_write_complete_ms", "first_response_byte_ms", "ended_ms"):
            for bad in (None, True, float("nan"), float("inf"), -1):
                value = cohort(); value["requests"][0][field] = bad; self.rejected(value)
            value = cohort(); value["requests"][0].pop(field); self.rejected(value)

    def test_inverted_and_nonpositive_sent_intervals_refuse(self):
        for field, bad in (("connected_ms", 9), ("client_write_complete_ms", 10.5),
                           ("first_response_byte_ms", 11), ("ended_ms", 13)):
            value = cohort(); value["requests"][0][field] = bad; self.rejected(value)
        value = cohort(); value["requests"][0].update(client_write_complete_ms=20, first_response_byte_ms=20); self.rejected(value)

    def test_transport_cleanup_timeout_and_oracle_errors_refuse_complete_response(self):
        for field, bad in (("capture_error", "ReadError"), ("connection_cleanup_failed", True),
                           ("run_deadline_cancelled", True), ("oracle_verdict", "failed"),
                           ("oracle_error_type", "ValueError"), ("evaluation_fallbacks", ["source injection"])):
            value = cohort(); value["requests"][0][field] = bad; self.rejected(value)
        value = cohort(); value["requests"][0]["independent_response_verdict"]["precision_at_k"] = 1; self.rejected(value)

    def test_barrier_missing_caller_and_sequential_order_refuse(self):
        value = cohort(); value["callers"].pop(); self.rejected(value)
        value = cohort(); value["callers"][0]["ready_ms"] = 2; self.rejected(value)
        value = cohort(); value["callers"][0]["finished_ms"] = 120001; self.rejected(value)
        value = cohort(); first = value["requests"][0]; second = next(row for row in value["requests"] if row["caller_id"] == first["caller_id"] and row["ordinal"] == 1)
        for key in ("started_ms", "connected_ms", "client_write_complete_ms", "first_response_byte_ms", "ended_ms"): second[key] -= 15
        self.rejected(value)

    def test_ready140_but_request_peak139_is_unmet_even_if_report_claims140(self):
        value = cohort(); caller = value["callers"][0]["id"]
        for row in value["requests"]:
            if row["caller_id"] == caller:
                for key in ("started_ms", "connected_ms", "client_write_complete_ms", "first_response_byte_ms", "ended_ms"): row[key] += 200
        next(row for row in value["callers"] if row["id"] == caller)["finished_ms"] += 200
        self.rejected(value)
        value.update(peak_in_flight=139, peak_sent_waiting_response=139); self.rejected(value)

    def test_lifetime140_with_sent_waiting139_is_unmet(self):
        value = cohort(); caller = value["callers"][0]["id"]
        for row in value["requests"]:
            start = row["started_ms"]
            if row["caller_id"] == caller: row.update(client_write_complete_ms=start+9, first_response_byte_ms=start+9.5)
            else: row["ended_ms"] = start+8
        value["peak_sent_waiting_response"] = 139
        self.rejected(value)

    def test_equal_end_start_does_not_inflate_client_overlap(self):
        self.assertEqual(workload.interval_peak([(0, 1), (1, 2)]), 1)
        self.assertEqual(workload.interval_peak([(0, 2), (1, 3)]), 2)
        with self.assertRaises(ValueError): workload.interval_peak([(1, 1)])

    def test_successful_only_latency_or_wrong_cutoff_cannot_pass(self):
        value = cohort()
        # Every attempt remains present. Stretch one complete ordinal for all
        # callers past the strict p95 threshold, retaining valid ordered times.
        for row in value["requests"]:
            if row["ordinal"] == 3:
                row["ended_ms"] = row["started_ms"] + 2000
                row["first_response_byte_ms"] = row["ended_ms"] - 1
        for caller in value["callers"]: caller["finished_ms"] = 2071
        self.rejected(value)

    def test_original_mrr_threshold_is_not_lowered_for_more_callers(self):
        value = cohort(); frozen = workload.corpus(expected.PROFILE_ID)
        queries = {row["id"]: row for row in frozen["queries"]}
        for row in value["requests"][:120]:
            query = queries[row["query_id"]]
            row["response"] = response_for(query, distractors=1)
            row["independent_response_verdict"] = workload.workload_response(query, row["response"], frozen)
        self.rejected(value)

    def test_json_capture_bound_counts_utf8_and_never_truncates(self):
        value = {"original": "é" * 100}
        size = len(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode())
        self.assertEqual(workload.bounded_json_bytes(value, size), size)
        with self.assertRaises(ValueError): workload.bounded_json_bytes(value, size - 1)
        with self.assertRaises(ValueError): workload.bounded_json_bytes({"value": float("nan")}, 1000)

    def test_complete_wire_response_does_not_replace_known_answer(self):
        value = cohort(); row = value["requests"][0]
        queries = workload.corpus()["queries"]
        target = next(q for q in queries if q["id"] == row["query_id"])
        required = lambda query: {(item["key"], item["revision"]) for item in query["required"]}
        # Different intent IDs can share the same valid full-source evidence.
        # This negative control must actually omit the original known answer.
        wrong = next(q for q in queries if q["space"] == row["space"]
                     and required(q).isdisjoint(required(target)))
        self.assertTrue(required(wrong).isdisjoint(required(target)))
        body = response_for(wrong)["body"]
        body["coverage"].update(row["response"]["body"]["coverage"])
        row["response"] = captured(body)
        self.assertTrue(row["response"]["framing_complete"])
        self.assertTrue(row["response"]["raw_body_complete"])
        with self.assertRaisesRegex(ValueError, "required workload evidence absent"):
            workload.evaluate_cohort(value)


if __name__ == "__main__":
    unittest.main()
