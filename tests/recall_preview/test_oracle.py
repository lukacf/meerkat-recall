"""Evaluator mutation tests only: passing these never qualifies the product."""

import copy
import json
import unittest

import fixtures
import oracle


def evidence(source: dict) -> dict:
    return {"key": source["key"], "revision": source["revision"], "space": source["space"],
            "start": 0, "end": len(source["text"].encode()), "quote": source["text"],
            "quote_hash": oracle.sha256(source["text"].encode()),
            "revision_hash": oracle.sha256(source["text"].encode())}


def known_answer() -> tuple[dict, dict, dict]:
    corpus = fixtures.corpus()
    query = copy.deepcopy(corpus["queries"][0])
    source = next(source for source in corpus["sources"] if source["revision"] == "heat-v2")
    result = {"key": source["key"], "revision": source["revision"], "space": source["space"],
              "published_revision": source["revision"], "desired_revision": source["revision"],
              "processing_state": "complete", "evidence": [evidence(source)]}
    capture = {"query": query["query"], "space": query["space"], "k": query["k"],
               "outcome": "complete", "evaluation_fallbacks": [], "results": [result]}
    return query, capture, corpus


def canonical_capture() -> dict:
    effects = ["assertion/a", "entity/a", "entity/b", "rel/a", "event/a", "freshness/a"]
    hashes = {effect: oracle.sha256(f"complete semantic row: {effect}".encode()) for effect in effects}
    initial = {effect: None for effect in effects}
    empty = {"effect_ids": [], "effect_hashes": initial.copy(), "receipt_complete": False, "outbox_complete": False}
    return {"expected_effect_ids": effects, "expected_effect_hashes": hashes, "initial_effect_hashes": initial,
            "snapshots": [empty, copy.deepcopy(empty),
            {"effect_ids": effects.copy(), "effect_hashes": hashes.copy(), "receipt_complete": True, "outbox_complete": True}],
            "first_complete_receipt": {"id": "receipt-a", "generation": "g1"},
            "replay_receipt": {"id": "receipt-a", "generation": "g1"},
            "stale_attempt_effect_ids": [], "stale_head_effect_ids": []}


def checkpoint_capture() -> dict:
    return {"initial_cursor": "cursor-10", "deliveries": [
        {"event_id": "event-11", "event_cursor": "cursor-11", "success": False,
         "cursor_before": "cursor-10", "cursor_after": "cursor-10", "after_restart": False,
         "new_effect_count": 0},
        {"event_id": "event-11", "event_cursor": "cursor-11", "success": True,
         "cursor_before": "cursor-10", "cursor_after": "cursor-11", "after_restart": True,
         "new_effect_count": 1},
        {"event_id": "event-11", "event_cursor": "cursor-11", "success": True,
         "cursor_before": "cursor-11", "cursor_after": "cursor-11", "after_restart": False,
         "new_effect_count": 0}]}


def erase_capture() -> dict:
    names = ["source", "source_revision", "doc", "doc_revision", "doc_node", "node_embedding",
             "work_artifact", "work_item", "work_item_archive", "work_run", "work_run_archive",
             "assertion", "exclusive_graph", "knowledge_view_revision"]
    return {"exclusive_ids": ["source-A-only"], "independently_supported_ids": ["A-and-B"],
            "post_acceptance_visibility": [{"visible_ids": ["A-and-B"]}, {"visible_ids": ["A-and-B"]}],
            "delayed_attempt_outcome": "conflict", "other_space_unchanged": True,
            "physical_payload_rows": {name: 0 for name in names}, "backup_retention_seconds": 86400,
            "retention_enforced": True, "cleanup_state": "erased", "journal_payload_free": True,
            "processor_retention_boundary": "Synthetic fixture; no external processor called",
            "journal_survived_old_snapshot": True, "ready_before_replay": False,
            "reads_before_replay": "unavailable", "replay_complete": True,
            "restored_erased_payload_rows": 0}


class EvidenceOracleTests(unittest.TestCase):
    def test_known_answer_uses_fixed_cutoff(self):
        query, capture, corpus = known_answer()
        self.assertEqual(oracle.retrieval(query, capture, corpus)["precision_at_k"], 1 / 3)

    def test_original_query_cannot_be_replaced_by_scenario_hint(self):
        query, capture, corpus = known_answer()
        capture["query"] = "home/heat boiler appointment 19 October"
        with self.assertRaisesRegex(oracle.Rejected, "original query"):
            oracle.retrieval(query, capture, corpus)

    def test_empty_search_does_not_pass_positive_query(self):
        query, capture, corpus = known_answer()
        capture["results"] = []
        with self.assertRaisesRegex(oracle.Rejected, "not retrieved"):
            oracle.retrieval(query, capture, corpus)

    def test_known_doc_fetch_cannot_repair_sparse_retrieval(self):
        query, capture, corpus = known_answer()
        capture["evaluation_fallbacks"] = ["get_doc(home/heat)"]
        with self.assertRaisesRegex(oracle.Rejected, "fallback"):
            oracle.retrieval(query, capture, corpus)

    def test_unavailable_is_not_negative_query_success(self):
        query, capture, corpus = known_answer()
        query = corpus["queries"][-1]
        capture.update(query=query["query"], results=[], outcome="unavailable")
        with self.assertRaisesRegex(oracle.Rejected, "not zero"):
            oracle.retrieval(query, capture, corpus)

    def test_stale_revision_cannot_hide_behind_ready_space(self):
        query, capture, corpus = known_answer()
        capture["results"][0]["desired_revision"] = "heat-v3"
        with self.assertRaisesRegex(oracle.Rejected, "stale"):
            oracle.retrieval(query, capture, corpus)

    def test_wrong_space_rejected_even_with_same_source_key(self):
        query, capture, corpus = known_answer()
        capture["results"][0]["space"] = "qualification-business"
        with self.assertRaisesRegex(oracle.Rejected, "cross-space"):
            oracle.retrieval(query, capture, corpus)

    def test_unicode_offsets_are_bytes_not_character_indices(self):
        source = next(source for source in fixtures.corpus()["sources"] if source["revision"] == "oriole-v2")
        span = evidence(source)
        oracle.exact_evidence(span, source, source["space"])
        span["start"] = 1
        with self.assertRaisesRegex(oracle.Rejected, "UTF-8"):
            oracle.exact_evidence(span, source, source["space"])

    def test_quote_and_hash_checked_independently(self):
        source = fixtures.corpus()["sources"][0]
        for change in ({"quote": "a fabricated date"}, {"quote_hash": "derived:abc"}, {"end": 0}, {"start": True}):
            with self.subTest(change=change), self.assertRaises(oracle.Rejected):
                oracle.exact_evidence(evidence(source) | change, source, source["space"])

    def test_duplicate_rank_does_not_inflate_precision(self):
        query, capture, corpus = known_answer()
        capture["results"] *= 2
        with self.assertRaisesRegex(oracle.Rejected, "duplicate"):
            oracle.retrieval(query, capture, corpus)


class DurabilityOracleTests(unittest.TestCase):
    def test_complete_atomic_retry_observation(self):
        oracle.publication(canonical_capture())

    def test_assertion_only_retry_is_not_completion(self):
        value = canonical_capture()
        value["snapshots"][1]["effect_ids"] = ["assertion/a"]
        self.assertEqual(oracle.evaluate("publication", value)["verdict"], "failed")

    def test_receipt_and_outbox_both_required(self):
        for field in ("receipt_complete", "outbox_complete"):
            value = canonical_capture()
            value["snapshots"][-1][field] = False
            self.assertEqual(oracle.evaluate("publication", value)["verdict"], "failed")

    def test_correct_ids_cannot_hide_wrong_canonical_contents(self):
        value = canonical_capture()
        value["snapshots"][-1]["effect_hashes"]["entity/a"] = oracle.sha256(b"old semantic label")
        with self.assertRaisesRegex(oracle.Rejected, "canonical content"):
            oracle.publication(value)

    def test_stale_same_worker_effect_is_not_ignored(self):
        value = canonical_capture()
        value["stale_attempt_effect_ids"] = ["event/stale"]
        self.assertEqual(oracle.evaluate("publication", value)["verdict"], "failed")

    def test_failed_delivery_then_restart_and_duplicate(self):
        oracle.checkpoint(checkpoint_capture())

    def test_failure_cannot_advance_cursor(self):
        value = checkpoint_capture()
        value["deliveries"][0]["cursor_after"] = "cursor-11"
        with self.assertRaisesRegex(oracle.Rejected, "failed consumer"):
            oracle.checkpoint(value)

    def test_restart_cannot_skip_failed_event(self):
        value = checkpoint_capture()
        value["deliveries"][1]["event_id"] = "event-12"
        self.assertEqual(oracle.evaluate("checkpoint", value)["verdict"], "failed")

    def test_duplicate_delivery_must_be_idempotent(self):
        value = checkpoint_capture()
        value["deliveries"][2]["new_effect_count"] = 1
        with self.assertRaisesRegex(oracle.Rejected, "duplicate event"):
            oracle.checkpoint(value)

    def test_erase_physical_inventory_and_replay_are_separate(self):
        value = erase_capture()
        self.assertEqual(oracle.evaluate("erase", value)["verdict"], "passed")
        value["ready_before_replay"] = True
        # OB3 physical deletion+retention gate does not silently acquire replay.
        self.assertEqual(oracle.evaluate("erase", value)["verdict"], "passed")
        self.assertEqual(oracle.evaluate("restore_replay", value)["verdict"], "failed")

    def test_soft_delete_alone_does_not_pass_erasure(self):
        value = erase_capture()
        value["physical_payload_rows"]["doc_revision"] = 1
        self.assertEqual(oracle.evaluate("erase", value)["verdict"], "failed")

    def test_independent_support_must_survive_erasure(self):
        value = erase_capture()
        value["post_acceptance_visibility"][-1]["visible_ids"] = []
        self.assertEqual(oracle.evaluate("erase", value)["verdict"], "failed")

    def test_payload_archive_omission_cannot_pass(self):
        value = erase_capture()
        del value["physical_payload_rows"]["work_item_archive"]
        self.assertEqual(oracle.evaluate("erase", value)["verdict"], "failed")

    def test_retention_must_be_finite_and_enforced(self):
        for change in ({"backup_retention_seconds": None}, {"backup_retention_seconds": 0}, {"retention_enforced": False}):
            self.assertEqual(oracle.evaluate("erase", erase_capture() | change)["verdict"], "failed")


class PrerequisiteTests(unittest.TestCase):
    def test_every_missing_observation_fails_closed(self):
        for kind in oracle.CHECKS:
            with self.subTest(kind=kind):
                self.assertEqual(oracle.evaluate(kind, {})["verdict"], "failed")

    def test_shape_correct_fake_vectors_cannot_qualify_semantics(self):
        value = {"provider_mode": "stub", "provider_request_receipts": ["fake"],
                 "original_query_scores_passed": True, "negative_controls_passed": True,
                 "fake_vector_control_rejected": True, "dimensions": 3,
                 "vectors": [[0.1, 0.8, 0.3], [0.4, 0.2, 0.9]]}
        with self.assertRaisesRegex(oracle.Rejected, "stub/fake"):
            oracle.semantic_provider(value)

    def test_authored_load_fixture_is_not_140_measured_calls(self):
        value = {"agreed_p95_ms": 1000, "agreed_p99_ms": 2000, "measured": False}
        with self.assertRaisesRegex(oracle.Rejected, "not a measurement"):
            oracle.workload(value)

    def test_workload_requires_independent_pre_run_contract(self):
        with self.assertRaisesRegex(oracle.Rejected, "separate agreed"):
            oracle.workload({"measured": True, "workload_contract": {"minimum_peak_in_flight": 1}})

    def test_worker_count_is_not_an_invented_concurrency_requirement(self):
        contract, capture = workload_capture()
        # Four deliberately sequential captured calls, agreed independently of
        # OB3's reported worker count. This is evaluator data, not a load run.
        oracle.workload(capture, contract)
        self.assertEqual(oracle.evaluate("workload", capture, workload_contract=contract)["verdict"], "passed")

    def test_workload_recomputes_overlap_and_checks_exact_contract(self):
        contract, capture = workload_capture()
        for mutation in ({"peak_in_flight": 140}, {"workload_contract_sha256": "0" * 64}):
            with self.subTest(mutation=mutation), self.assertRaises(oracle.Rejected):
                oracle.workload(capture | mutation, contract)
        higher = contract | {"minimum_peak_in_flight": 2}
        capture["workload_contract_sha256"] = workload_digest(higher)
        with self.assertRaisesRegex(oracle.Rejected, "overlap"):
            oracle.workload(capture, higher)

    def test_workload_missing_invalid_or_failed_samples_cannot_pass(self):
        contract, capture = workload_capture()
        changes = [{"ended_ms": 0}, {"ended_ms": float("nan")},
                   {"response_bytes": 4097}, {"oracle_verdict": "failed"},
                   {"space": "default"}, {"id": "request-1"}]
        for change in changes:
            value = copy.deepcopy(capture)
            value["requests"][0].update(change)
            with self.subTest(change=change), self.assertRaises(oracle.Rejected):
                oracle.workload(value, contract)
        with self.assertRaisesRegex(oracle.Rejected, "sample count"):
            oracle.workload(capture | {"requests": capture["requests"][:-1]}, contract)

    def test_size_boundary_counts_utf8_bytes(self):
        for size in (1_999_999, 2_000_000, 2_000_001):
            for multibyte in (False, True):
                self.assertEqual(len(fixtures.sized_text(size, multibyte).encode()), size)
        self.assertEqual(len(fixtures.sized_text(2_000_000, True)), 1_000_000)

    def test_ob3_has_50_explicit_sources_per_distinct_space(self):
        first, second = [row["id"] for row in fixtures.corpus()["spaces"]]
        self.assertEqual(len(fixtures.structured_sources(first)), 50)
        self.assertEqual(len(fixtures.structured_sources(second)), 50)
        self.assertNotEqual(fixtures.structured_sources(first), fixtures.structured_sources(second))
        with self.assertRaises(ValueError):
            fixtures.structured_sources("default")

    def test_degraded_source_never_constructs_ingest_request(self):
        source = next(row for row in fixtures.corpus()["sources"] if row["kind"] == "degraded-withheld")
        with self.assertRaisesRegex(ValueError, "withhold"):
            fixtures.source_request(source)


def workload_digest(contract):
    return oracle.sha256(json.dumps(contract, sort_keys=True, separators=(",", ":")).encode())


def workload_capture():
    contract = {"format": "recall-preview-workload/v1", "consumer_workload_reference": "synthetic-evaluator-only",
                "request_count": 4, "minimum_peak_in_flight": 1, "p95_ms": 100, "p99_ms": 100,
                "request_timeout_ms": 200, "response_budget_bytes": 4096, "duration_budget_ms": 1000,
                "spaces": ["qualification-household", "qualification-business"]}
    capture = {"measured": True, "workload_contract_sha256": workload_digest(contract),
               "sources_per_space": {space: 50 for space in contract["spaces"]}, "peak_in_flight": 1,
               "requests": [{"id": f"request-{index}", "space": contract["spaces"][index % 2],
                             "started_ms": index * 100, "ended_ms": (index + 1) * 100,
                             "response_bytes": 100, "oracle_verdict": "passed"} for index in range(4)]}
    return contract, capture


if __name__ == "__main__":
    unittest.main()
