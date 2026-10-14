"""Source-only pure controls; no consumer, server, subprocess or network."""
import copy
import json
import unittest

from ob3_outage_oracle import evaluate_receipt_bytes, POR, PUSH, REASONS, PRODUCER_CHECKS


def fixture():
    expected = {"format": "recall-ob3-outage-expectations/v1", "run_id": "independent-run-1",
                "ob3_git_sha": "a" * 40, "recall_contract_version": "recall-preview-1",
                "recall_endpoint_state": "refused", "batch_kind": "review",
                "requested_worker_ids": ["review:a", "review:b", "review:idle"],
                "expected_participating_workers": ["review:a", "review:b"],
                "por_baseline": {"failed": 0, "degraded": 0, "fetched": 10, "inserted": 4},
                "maximum_logical_reads": 6, "earliest_started_at": "2026-10-07T00:00:00Z",
                "latest_finished_at": "2026-10-07T00:05:00Z"}
    receipt = {key: expected[key] for key in ("run_id", "ob3_git_sha", "recall_contract_version", "recall_endpoint_state")}
    receipt.update(receipt_version="ob3-recall-outage-2",
                   batch={"kind": "review", "requested_workers": 3, "completed_workers": 3, "failed_workers": 0,
                          "started_at": "2026-10-07T00:00:01Z", "finished_at": "2026-10-07T00:00:20Z"},
                   expected_participating_workers=["review:a", "review:b"],
                   participating_workers=["review:a", "review:b"],
                   read_total={"reads": 2, "evidence": 0, "size_retries": 0,
                               "fallbacks": {**dict.fromkeys(REASONS, 0), "refused": 2}},
                   workers={key: {"reads": 1, "evidence": 0, "fallbacks": {"refused": 1}}
                            for key in ["review:a", "review:b"]},
                   por_push={"baseline": copy.deepcopy(expected["por_baseline"]),
                             "with_recall": copy.deepcopy(expected["por_baseline"]), "recall": dict.fromkeys(PUSH, 0)},
                   verdict={"status": "PASS", "checks": dict.fromkeys(PRODUCER_CHECKS, True)})
    return receipt, expected


def evaluate(receipt, expected):
    return evaluate_receipt_bytes(json.dumps(receipt).encode(), json.dumps(expected).encode())


class OriginalReceiptControls(unittest.TestCase):
    def test_original_v1_is_historical_not_new_acceptance(self):
        receipt, expected = fixture()
        receipt["receipt_version"] = "ob3-recall-outage-1"
        for key in ["cross_total", "participants_match_reads", "no_duplicate_participants"]:
            del receipt["verdict"]["checks"][key]
        result = evaluate(receipt, expected)
        self.assertEqual(result["verdict"], "unmet")
        self.assertEqual(result["errors"], ["receipt_version"])

    def test_actual_participant_subset_and_sparse_worker_reasons(self):
        receipt, expected = fixture()
        self.assertEqual(evaluate(receipt, expected)["verdict"], "pass")

    def test_individually_balanced_but_unsummed_totals_fail(self):
        receipt, expected = fixture()
        receipt["read_total"]["reads"] = 3
        receipt["read_total"]["fallbacks"]["refused"] = 3
        result = evaluate(receipt, expected)
        self.assertTrue(result["checks"]["accounting_total"])
        self.assertTrue(result["checks"]["accounting_per_worker"])
        self.assertIn("summed_worker_totals", result["errors"])

    def test_equal_count_wrong_reason_distribution_fails(self):
        receipt, expected = fixture()
        receipt["read_total"]["fallbacks"].update(refused=0, timeout=2)
        self.assertIn("summed_worker_totals", evaluate(receipt, expected)["errors"])

    def test_positive_workers_must_equal_original_participants(self):
        receipt, expected = fixture()
        receipt["workers"]["review:b"] = {"reads": 0, "evidence": 0, "fallbacks": {}}
        receipt["workers"]["review:a"] = {"reads": 2, "evidence": 0, "fallbacks": {"refused": 2}}
        self.assertIn("independent_participants", evaluate(receipt, expected)["errors"])

    def test_candidate_cannot_choose_smaller_expected_set(self):
        receipt, expected = fixture()
        receipt["participating_workers"] = ["review:a"]
        receipt["expected_participating_workers"] = ["review:a"]
        self.assertIn("independent_participants", evaluate(receipt, expected)["errors"])

    def test_original_duplicate_arrays_are_not_silently_deduplicated(self):
        for key in ["participating_workers", "expected_participating_workers"]:
            receipt, expected = fixture()
            receipt[key].append("review:a")
            self.assertEqual(evaluate(receipt, expected)["errors"], ["duplicate_worker_identity"])

    def test_duplicate_worker_json_key_is_refused(self):
        receipt, expected = fixture()
        raw = json.dumps(receipt).encode().replace(b'"workers": {', b'"workers": {"review:a": {},', 1)
        self.assertEqual(evaluate_receipt_bytes(raw, json.dumps(expected).encode())["errors"], ["duplicate_json_key"])

    def test_unknown_and_noninteger_counts_are_unmet(self):
        for value in [True, -1, 1.0, 2**64]:
            receipt, expected = fixture()
            receipt["read_total"]["reads"] = value
            self.assertEqual(evaluate(receipt, expected)["verdict"], "unmet")
        receipt, expected = fixture()
        receipt["workers"]["review:a"]["fallbacks"]["not_configured"] = 1
        self.assertEqual(evaluate(receipt, expected)["verdict"], "unmet")

    def test_missing_batch_or_baseline_cannot_pass(self):
        for key in ["batch", "baseline"]:
            receipt, expected = fixture()
            if key == "batch":
                receipt[key] = None
            else:
                receipt["por_push"][key] = None
            self.assertEqual(evaluate(receipt, expected)["verdict"], "unmet")

    def test_candidate_cannot_rewrite_both_baselines(self):
        receipt, expected = fixture()
        for key in ["baseline", "with_recall"]:
            receipt["por_push"][key]["fetched"] = 11
        self.assertIn("original_por_baseline", evaluate(receipt, expected)["errors"])

    def test_all_requested_workers_and_original_interval_remain_required(self):
        receipt, expected = fixture()
        receipt["batch"]["completed_workers"] = 2
        receipt["batch"]["finished_at"] = "2026-10-07T00:05:01Z"
        result = evaluate(receipt, expected)
        self.assertIn("all_requested_completed", result["errors"])
        self.assertIn("original_interval", result["errors"])

    def test_missing_expected_sources_is_not_configured_outage_participation(self):
        receipt, expected = fixture()
        for worker in receipt["workers"].values():
            worker["fallbacks"] = {"no_expected_sources": 1}
        receipt["read_total"]["fallbacks"].update(refused=0, no_expected_sources=2)
        self.assertIn("configured_expected_sources", evaluate(receipt, expected)["errors"])

    def test_forged_producer_pass_does_not_establish_independent_pass(self):
        receipt, expected = fixture()
        receipt["read_total"]["reads"] = 0
        result = evaluate(receipt, expected)
        self.assertEqual(result["producer_reported_status"], "PASS")
        self.assertEqual(result["verdict"], "fail")
        self.assertFalse(result["product_acceptance"])

    def test_hashes_preserve_original_bytes_not_parsed_reencoding(self):
        receipt, expected = fixture()
        compact = json.dumps(receipt, separators=(",", ":")).encode()
        spaced = json.dumps(receipt, indent=2).encode()
        expectations = json.dumps(expected).encode()
        left = evaluate_receipt_bytes(compact, expectations)
        right = evaluate_receipt_bytes(spaced, expectations)
        self.assertEqual(left["verdict"], right["verdict"])
        self.assertNotEqual(left["receipt_sha256"], right["receipt_sha256"])


if __name__ == "__main__":
    unittest.main()
