"""Closed-world source controls; these are never installed-product observations."""
from __future__ import annotations

import copy
import concurrent.futures
import hashlib
import json
import math
from pathlib import Path
import struct
import sys
import tempfile
import threading
import types
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import installed_fake_vectors as driver
import installed_fake_vectors_contract as contract
import installed_fake_vectors_spy as spy
import full_workload_spy as original_spy


class SeededContractControls(unittest.TestCase):
    def setUp(self):
        self.corpus = json.loads(driver.CORPUS.read_text())
        self.recipe = contract.recipe(self.corpus, 17, "local384")

    def test_original_three_seeds_and_two_matching_configurations(self):
        for seed in (17, 219, 20261006):
            for mode, dimensions in (("local384", (384, 384)), ("primary", (1536, 3072))):
                value = contract.recipe(self.corpus, seed, mode)
                self.assertEqual(tuple(r["output"]["dimensions"] for r in value["rules"]), dimensions)
                self.assertEqual(contract.validate_recipe(value, contract.digest(value), self.corpus), value)

    def test_original_document_and_query_ordinals_are_frozen(self):
        rows = {(rule["space"], row["text"]): row for rule in self.recipe["rules"] for row in rule["output"]["mapping"]}
        self.assertEqual(len(rows), 52)
        for ordinal, source in enumerate(self.corpus["sources"]):
            self.assertEqual(rows[(source["space"], source["text"])]["stream"], ordinal)
        for ordinal, query in enumerate(self.corpus["queries"]):
            self.assertEqual(rows[(query["space"], query["query"])]["stream"], 10000 + ordinal)

    def test_source_text_or_query_change_refused_before_mapping(self):
        for collection, key in (("sources", "text"), ("queries", "query")):
            changed = copy.deepcopy(self.corpus)
            changed[collection][0][key] += " rescue anchor"
            with self.assertRaises(ValueError):
                contract.recipe(changed, 17, "local384")

    def test_unknown_seed_boolean_seed_and_configuration_refused(self):
        for seed, mode in ((18, "local384"), (True, "local384"), (17, "arbitrary")):
            with self.assertRaises(ValueError):
                contract.recipe(self.corpus, seed, mode)

    def test_recipe_mutations_refused_even_with_recomputed_hash(self):
        for mutate in (lambda v: v.update(extra="unreviewed"),
                       lambda v: v["rules"][0].update(path="/v1/responses"),
                       lambda v: v["rules"][0]["output"].update(dimensions=8),
                       lambda v: v["rules"][0]["output"]["mapping"][0].update(stream=999),
                       lambda v: v["rules"][0].update(credential_sha256="0" * 64)):
            value = copy.deepcopy(self.recipe)
            mutate(value)
            with self.assertRaises(ValueError):
                contract.validate_recipe(value, contract.digest(value), self.corpus)

    def test_counter_vectors_follow_independent_coordinate_formula(self):
        seed, stream, dimensions = 219, 10004, 384
        values = []
        for coordinate in range(dimensions):
            raw = hashlib.sha256(b"semantic-index-negative-v1" + seed.to_bytes(8, "little")
                + stream.to_bytes(8, "little") + coordinate.to_bytes(8, "little")).digest()
            scalar = ((int.from_bytes(raw[:8], "little") >> 11) / 9007199254740992.0) * 2 - 1
            values.append(struct.unpack("<f", struct.pack("<f", scalar))[0])
        norm = math.sqrt(sum(x * x for x in values))
        expected = [struct.unpack("<f", struct.pack("<f", x / norm))[0] for x in values]
        actual = contract.vector(seed, stream, dimensions)
        self.assertEqual(actual, expected)
        self.assertAlmostEqual(sum(x * x for x in actual), 1.0, places=6)
        self.assertNotEqual(actual, contract.vector(17, stream, dimensions))
        self.assertNotEqual(actual, contract.vector(seed, 4, dimensions))

    def test_exact_embedding_batch_preserves_original_order(self):
        rule = self.recipe["rules"][0]
        texts = [rule["output"]["mapping"][2]["text"], rule["output"]["mapping"][0]["text"]]
        content_type, raw = contract.response(rule, {"model": rule["model"], "input": texts, "dimensions": 384})
        body = json.loads(raw)
        self.assertEqual(content_type, "application/json")
        self.assertEqual([row["index"] for row in body["data"]], [0, 1])
        self.assertEqual(body["data"][0]["embedding"], contract.vector(17, 2, 384))
        self.assertNotEqual(body["data"][0]["embedding"], body["data"][1]["embedding"])

    def test_unmapped_text_dimension_model_and_format_refused(self):
        rule = self.recipe["rules"][0]
        original = {"model": rule["model"], "input": [rule["output"]["mapping"][0]["text"]], "dimensions": 384}
        for mutation in ({"input": ["undeclared text"]}, {"input": original["input"][0]},
                         {"dimensions": 1536}, {"model": "different"}, {"encoding_format": "base64"},
                         {"surprise": True}):
            with self.assertRaises(ValueError):
                contract.response(rule, {**original, **mutation})

    def scores(self, passing=True):
        return [{"id": q["id"], "retrieval_pass": passing, "required_recall_at_3": 1,
                 "reciprocal_rank": 1.0 if passing else 0.0} for q in self.corpus["queries"]]

    def test_fake_evidence_label_cannot_turn_numerical_pass_into_negative(self):
        result = contract.numerical_verdict(self.corpus, self.scores())
        self.assertTrue(result["numerical_retrieval_pass"])
        self.assertFalse(result["negative_control_pass"])
        self.assertFalse(result["product_acceptance"])

    def test_original_combined_threshold_not_strengthened_to_two_failures(self):
        scores = self.scores()
        scores[0]["retrieval_pass"] = False
        self.assertTrue(contract.numerical_verdict(self.corpus, scores)["negative_control_pass"])
        scores = self.scores()
        for row in scores:
            row["reciprocal_rank"] = 0.1
        self.assertTrue(contract.numerical_verdict(self.corpus, scores)["negative_control_pass"])

    def test_missing_or_reordered_numerical_queries_refused(self):
        for scores in (self.scores()[:-1], list(reversed(self.scores()))):
            with self.assertRaises(ValueError):
                contract.numerical_verdict(self.corpus, scores)

    def test_partial_or_missing_coverage_is_not_numerical_failure(self):
        query = self.corpus["queries"][0]
        for response in ({"partial": True, "results": []}, {"partial": False, "results": [{"kind": "chunk"}] * 3, "coverage": {}}):
            with self.assertRaises(ValueError):
                contract.score(driver.primary, driver.semantic, self.corpus, query, response)

    def test_configuration_cannot_use_real_provider_or_foreign_partition(self):
        space = self.corpus["spaces"][0]
        endpoint = "http://127.0.0.1:19999/v1"
        actual = {"space_id": space["id"], "security": contract.security(space), "purpose": space["guidance"],
            "embedding_provider": "openai", "embedding_model": contract.model(384), "embedding_dims": 384,
            "processing": contract.processing(space, 384, endpoint)}
        contract.configuration(space, 384, endpoint, actual)
        actual["processing"]["connections"][space["id"] + "-negative"]["endpoint"] = "https://provider.invalid/v1"
        with self.assertRaises(ValueError):
            contract.configuration(space, 384, endpoint, actual)

    def test_existing_spy_defaults_remain_original_functions(self):
        self.assertIs(original_spy.ControlledSpy.validate_recipe, original_spy.validate_recipe)
        self.assertIs(original_spy.ControlledSpy.provider_response, original_spy.provider_response)
        rule = {"mode": "embedding", "output": {"dimensions": 1536}}
        _, raw = original_spy.ControlledSpy.provider_response(rule, {"input": ["old fixture"], "model": "old"})
        self.assertEqual(json.loads(raw)["data"][0]["embedding"], [1.0] + [0.0] * 1535)

    def test_existing_spy_rejects_new_seeded_recipe(self):
        with self.assertRaises(original_spy.Unmet):
            original_spy.ControlledSpy.validate_recipe(self.recipe, contract.digest(self.recipe))
        self.assertEqual(spy.SeededSpy.validate_recipe(self.recipe, contract.digest(self.recipe)), self.recipe)

    def test_actual_headerless_embedding_wire_uses_unique_credential_scope(self):
        instance = spy.SeededSpy.__new__(spy.SeededSpy)
        instance.recipe, instance.rule_counts = self.recipe, {}
        self.assertNotEqual(self.recipe["rules"][0]["credential_sha256"], self.recipe["rules"][1]["credential_sha256"])
        for rule in self.recipe["rules"]:
            observed = {key: rule[key] for key in ("credential_sha256", "model", "path")}
            observed["space"] = None
            selected, ordinal = instance.select(observed, {"model": rule["model"], "input": []})
            self.assertEqual(selected["space"], rule["space"])
            self.assertIsNone(observed["wire_space"])
            self.assertEqual(ordinal, 1)

    def test_conflicting_header_foreign_credential_and_nonembedding_path_refused(self):
        instance = spy.SeededSpy.__new__(spy.SeededSpy)
        instance.recipe, instance.rule_counts = self.recipe, {}
        rule = self.recipe["rules"][0]
        original = {key: rule[key] for key in ("credential_sha256", "model", "path")}
        for changes in ({"space": self.recipe["rules"][1]["space"]},
                        {"credential_sha256": "0" * 64}, {"path": "/v1/responses"}):
            with self.assertRaises(ValueError):
                instance.select({**original, "space": None, **changes}, {"model": rule["model"], "input": []})
        self.assertEqual(instance.rule_counts, {})

    def test_physical_query_omission_and_repetition_cannot_pass(self):
        instance = driver.InstalledFakeVectors.__new__(driver.InstalledFakeVectors)
        instance.recipe = self.recipe
        expected = {(rule["space"], item["text"]): 1 for rule in self.recipe["rules"] for item in rule["output"]["mapping"]}
        query = next((rule["space"], item["text"]) for rule in self.recipe["rules"]
                     for item in rule["output"]["mapping"] if item["kind"] == "query")
        for counts in ({k: v for k, v in expected.items() if k != query}, {**expected, query: 2}):
            instance.spy = types.SimpleNamespace(input_counts=lambda: ({}, counts))
            with self.assertRaises(ValueError):
                instance.verify_provider_inputs()

    def test_preexisting_store_refused_and_preidentity_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            instance = driver.InstalledFakeVectors.__new__(driver.InstalledFakeVectors)
            instance.root = Path(directory)
            instance.manifest = {"nonce": "original-nonce", "backend": "surreal-db", "seed": 17}
            instance.report = {}
            instance.save = mock.Mock()
            instance.store_identity("before")
            self.assertTrue(all(not row["exists"] for row in instance.report["store_before"]["stores"]))
            (instance.root / "service/native-store").mkdir(parents=True)
            with self.assertRaises(ValueError):
                instance.store_identity("before")

    def test_cancelled_or_uncaptured_provider_send_refuses_evidence(self):
        instance = spy.OwnedSpy.__new__(spy.OwnedSpy)
        for row in ({"request_complete": True, "response_complete": False, "status": 200},
                    {"request_complete": True, "response_complete": True, "status": 200, "connection_cleanup_failed": True}):
            instance.snapshot = lambda: {"error_type": None, "active": 0, "held_ids": [], "complete_calls": [row]}
            with self.assertRaises(ValueError):
                instance.input_counts()

    def test_live_spy_thread_cannot_report_clean_shutdown(self):
        instance = spy.OwnedSpy.__new__(spy.OwnedSpy)
        instance.closed, instance.loop, instance.spy = False, None, None
        instance.close_requested, instance.thread_error = threading.Event(), None
        instance.whole_deadline = driver.time.monotonic() + 1
        instance.thread = types.SimpleNamespace(ident=1, join=lambda timeout: None, is_alive=lambda: True)
        with self.assertRaises(ValueError):
            instance.close()
        self.assertFalse(instance.closed)

    def test_failed_close_future_still_stops_and_joins_owned_thread(self):
        instance = spy.OwnedSpy.__new__(spy.OwnedSpy)
        instance.closed, instance.thread_error = False, None
        instance.close_requested = threading.Event()
        instance.whole_deadline = driver.time.monotonic() + 1
        loop = instance.loop = mock.Mock()
        loop.is_running.return_value, loop.is_closed.return_value = True, False
        alive = [True]
        def join(timeout):
            self.assertGreaterEqual(timeout, 0)
            alive[0] = False
        instance.thread = types.SimpleNamespace(ident=1, join=mock.Mock(side_effect=join), is_alive=lambda: alive[0])
        async def close_spy():
            return None
        instance.spy = types.SimpleNamespace(close=close_spy, tasks=set())
        future = mock.Mock()
        future.result.side_effect = TimeoutError("original close future expired")
        def submit(coroutine, target):
            coroutine.close()
            self.assertIs(target, loop)
            return future
        with mock.patch.object(spy.asyncio, "run_coroutine_threadsafe", side_effect=submit):
            with self.assertRaisesRegex(TimeoutError, "original close future expired"):
                instance.close()
        loop.call_soon_threadsafe.assert_called_once_with(loop.stop)
        instance.thread.join.assert_called_once()
        future.cancel.assert_called_once()
        self.assertTrue(instance.close_requested.is_set())
        self.assertTrue(instance.closed)  # Ownership closed; the original failure still propagated.

    def test_startup_timeout_latch_prevents_late_listener_and_run_forever(self):
        instance = spy.OwnedSpy.__new__(spy.OwnedSpy)
        instance.closed, instance.loop, instance.spy, instance.thread_error = False, None, None, None
        instance.close_requested = threading.Event()
        instance.ready = concurrent.futures.Future()
        instance.whole_deadline = driver.time.monotonic() + 1
        loop, alive = mock.Mock(), [True]
        def delayed_start(timeout):
            self.assertTrue(instance.close_requested.is_set())
            instance._run()
            alive[0] = False
        instance.thread = types.SimpleNamespace(ident=1, join=mock.Mock(side_effect=delayed_start), is_alive=lambda: alive[0])
        with mock.patch.object(spy.asyncio, "new_event_loop", return_value=loop), \
             mock.patch.object(spy.asyncio, "set_event_loop"), \
             mock.patch.object(spy.asyncio, "all_tasks", return_value=set()), \
             mock.patch.object(spy, "Capture") as capture, mock.patch.object(spy, "SeededSpy") as server:
            instance.close()
        capture.assert_not_called()
        server.assert_not_called()
        loop.run_forever.assert_not_called()
        loop.close.assert_called_once()
        self.assertTrue(instance.closed)
        self.assertIsInstance(instance.ready.exception(), ValueError)

    def publication_fixture(self):
        source = self.corpus["sources"][0]
        space = next(row for row in self.corpus["spaces"] if row["id"] == source["space"])
        rendered = next(row for row in json.loads(driver.RENDERED.read_text())["sources"]
                        if (row["space"], row["source_key"], row["revision"]) ==
                           (source["space"], source["id"], source["revision"]))
        authority = {"space_id": space["id"], "deleted_at": None, "security": contract.security(space)}
        accepted = {"doc_id": "doc:original", "internal_source_id": "source:original"}
        doc = {**authority, "id": accepted["doc_id"], "source": accepted["internal_source_id"],
               "text": rendered["rendered_text"]}
        record = {**authority, "id": accepted["internal_source_id"], "external_id": source["id"],
                  "hash": driver.primary.sha(source["text"].encode()), "current_ingest_mode": "retrieval",
                  "lifecycle_status": "retrieval_ready"}
        pipeline = {"doc_id": accepted["doc_id"], "revision": "doc_revision:original",
                    "chunks": {"total": 2, "embedded": 2}, "nodes_count": 3}
        endpoint = "http://127.0.0.1:19999/v1"
        # Literal wire role comes from source-pinned Serialize/producer below,
        # independently of the rendered map's coordinate key root_span.
        nodes = [{**authority, "id": "doc_node:" + role, "role": role, "revision": pipeline["revision"],
                  "span": copy.deepcopy(span), "text": None,
                  "embedding_model": contract.partition_model(space, 384, endpoint)}
                 for role, span in (("document", rendered["root_span"]),
                                    ("paragraph", rendered["paragraph_spans"][0]),
                                    ("chunk", rendered["chunk_spans"][0]))]
        return [driver.primary, source, accepted, doc, record, pipeline, nodes, space, rendered, 384, endpoint]

    def test_actual_document_wire_preserves_original_rendered_root_span(self):
        values = self.publication_fixture()
        self.assertEqual(values[6][0]["role"], "document")
        self.assertEqual(values[6][0]["span"], values[8]["root_span"])
        plan = driver.plan()
        for authority in ("crates/types/src/records/doc_node.rs", "crates/pipeline/src/scheduler/evidence_work_kinds.rs"):
            self.assertEqual(plan["input_sha256"][authority], driver.core.file_digest(driver.ROOT / authority))
        contract.retrieval_publication(*values)

    def test_root_missing_duplicate_and_short_document_wire_refused(self):
        for change in (lambda nodes: nodes[0].update(role="root"), lambda nodes: nodes.pop(0),
                       lambda nodes: nodes[1].update(role="document"),
                       lambda nodes: nodes[0]["span"].update(end=nodes[0]["span"]["end"] - 1)):
            values = self.publication_fixture()
            original_map = copy.deepcopy(values[8])
            change(values[6])
            with self.assertRaises(ValueError):
                contract.retrieval_publication(*values)
            self.assertEqual(values[8], original_map)

    def test_original_clock_is_reused_not_restarted(self):
        original = {"mono_before_ns": 10, "mono_after_ns": 11, "wall_ns": 22}
        manifest = {"expires_at": "2026-10-07T20:11:00Z", "original_allocation_clock": original}
        with mock.patch.object(driver.core.process_capture, "pin_allocation_clock", return_value=(original, 33)) as pin:
            self.assertEqual(driver.original_clock(manifest), (original, 33))
            self.assertIs(pin.call_args.kwargs["original_sample"], original)

    def test_original_offline_matrix_and_source_test_are_not_replaced(self):
        recipe = json.loads(driver.RECIPE.read_text())
        self.assertEqual(recipe["offline_control_preserved"]["dimensions"], [8, 384, 1536])
        self.assertEqual(recipe["offline_control_preserved"]["seeds"], [17, 219, 20261006])
        self.assertEqual(len(recipe["sources"]), 28)
        self.assertEqual(len(recipe["queries"]), 24)


if __name__ == "__main__":
    unittest.main()
