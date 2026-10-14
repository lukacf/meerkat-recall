"""Closed-world local driver controls. No child, socket, provider or model loads."""
from __future__ import annotations

import copy
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("tested_local_driver", HERE / "local_fastembed_driver.py")
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)
c = driver.contract


class LocalContracts(unittest.TestCase):
    def setUp(self):
        self.corpus = driver.core.read_json(driver.CORPUS)
        self.space = self.corpus["spaces"][0]
        self.query = self.corpus["queries"][0]

    def response(self):
        source = next(s for s in self.corpus["sources"] if s["id"] == "home/water")
        return {"results": [{"kind": "chunk", "text": source["text"], "evidence": [{
            "source_id": source["id"], "revision": source["revision"], "quote": source["text"],
            "span": {"start": 0, "end": len(source["text"].encode())}}]}], "partial": False,
            "coverage": {key: {"state": "complete", "requested_revision": revision,
                "desired_revision": revision, "published_revision": revision}
                for key, revision in driver.primary.current_inventory(self.corpus, self.query["space"]).items()}}

    def score(self, response):
        return c.query_observation(driver.primary, driver.semantic, self.corpus, self.query, response)

    def test_original_plan_contains_all28_revisions_and24_queries(self):
        plan = driver.plan()
        self.assertEqual(len(plan["expectations"]["sources"]), 28)
        self.assertEqual(len(plan["expectations"]["queries"]), 24)
        self.assertFalse(plan["product_acceptance"])
        self.assertEqual(plan["expectations"]["limits"], c.LIMITS)
        self.assertEqual(plan["input_sha256"]["tests/recall_preview/corpus.json"],
                         driver.core.file_digest(driver.core.fixtures.CORPUS_PATH))

    def test_exact_evidence_passes_retrieval_but_does_not_invent_groundedness(self):
        _, result = self.score(self.response())
        self.assertTrue(result["retrieval_pass"])
        self.assertTrue(result["completion_pass"])
        self.assertFalse(result["groundedness_pass"])

    def test_correct_source_id_cannot_rescue_forged_quote(self):
        response = self.response()
        response["results"][0]["evidence"][0]["quote"] = "forged answer"
        with self.assertRaises(ValueError):
            self.score(response)

    def test_cross_space_evidence_fails_original_oracle(self):
        response = self.response()
        response["results"][0]["evidence"][0]["source_id"] = "business/budget"
        with self.assertRaises(ValueError):
            self.score(response)

    def test_partial_or_wrong_current_revision_fails(self):
        for change in (lambda response: response.update(partial=True),
                       lambda response: response["coverage"]["home/water"].update(published_revision="old")):
            response = self.response()
            change(response)
            with self.assertRaises(ValueError):
                self.score(response)

    def test_unrelated_well_formed_hit_does_not_pass_quality_gate(self):
        response = self.response()
        source = next(s for s in self.corpus["sources"] if s["id"] == "home/water-distractor")
        response["results"] = [{"kind": "chunk", "text": source["text"],
            "evidence": [driver.controls.evidence(source)]}]
        _, result = self.score(response)
        self.assertFalse(result["retrieval_pass"])

    def test_original_seeded384_controls_reject_shape_valid_vectors(self):
        self.assertEqual(driver.controls.SEEDS, (17, 219, 20261006))
        for seed in driver.controls.SEEDS:
            value = driver.controls.negative_control(self.corpus, seed, 384)
            self.assertTrue(value["valid_finite_nonzero_unit_vectors"])
            self.assertTrue(value["negative_control_pass"])
            self.assertFalse(value["product_acceptance"])

    def test_replacement_retains_source_identity_with_explicit_original_generation(self):
        source = self.corpus["sources"][0]
        first = driver.primary.source_request(source, "surreal-db", "original-fresh-nonce", "2026-10-07T00:00:00Z",
                                               c.security(self.space), None)
        replacement = dict(source, revision="new-revision", text="replacement text")
        second = driver.primary.source_request(replacement, "surreal-db", "original-fresh-nonce", "2026-10-07T00:00:00Z",
                                                c.security(self.space), "original-generation")
        self.assertEqual(first["source"]["id"], second["source"]["id"])
        self.assertNotEqual(first["doc"]["id"], second["doc"]["id"])
        self.assertEqual(second["intake"]["expected_generation"], "original-generation")

    def test_readiness_preserves503_while_requiring_real_local_assets(self):
        body = {"space": self.space["id"], "storage_ready": True, "runtime_mode": "normal",
                "processing_generation": 1, "approved_operations": {}, "available_operations": {},
                "embedding_execution_recipes": {}}
        for operation in c.OPERATIONS:
            body["approved_operations"][operation] = True
            body["available_operations"][operation] = True
            body["embedding_execution_recipes"][operation] = {"partition": c.partition(self.space),
                "provenance": {"operation": operation, "config": {"space_id": self.space["id"], "generation": 1}}}
        for operation in ("extraction", "support_verification", "triage", "reindex_embedding"):
            body["approved_operations"][operation] = False
        response = {"status": 503, "body": body}
        c.readiness(self.space, response)
        body["available_operations"]["document_embedding"] = False
        with self.assertRaises(ValueError):
            c.readiness(self.space, response)

    def test_configuration_rejects_credential_fallback_and_profile_rotation(self):
        config = {"space_id": self.space["id"], "security": c.security(self.space),
                  "embedding_provider": "fastembed", "embedding_model": c.MODEL, "embedding_dims": 384,
                  "processing": c.processing(self.space)}
        c.configuration(self.space, config)
        for field, value in (("credential_ref", "REMOTE_API_KEY"), ("endpoint", "https://remote.invalid")):
            altered = copy.deepcopy(config)
            next(iter(altered["processing"]["connections"].values()))[field] = value
            with self.assertRaises(ValueError):
                c.configuration(self.space, altered)

    def publication(self):
        source = self.corpus["sources"][0]
        rendered = next(row for row in driver.core.read_json(driver.RENDERED)["sources"]
                        if (row["space"], row["source_key"], row["revision"])
                        == (source["space"], source["id"], source["revision"]))
        base = {"space_id": source["space"], "security": c.security(self.space), "deleted_at": None}
        accepted = {"doc_id": "doc:original", "internal_source_id": "source:original"}
        doc = dict(base, id="doc:original", source="source:original", text=rendered["rendered_text"])
        record = dict(base, id="source:original", external_id=source["id"],
                      hash=driver.primary.sha(source["text"].encode()),
                      current_ingest_mode="retrieval", lifecycle_status="retrieval_ready")
        pipeline = {"doc_id": "doc:original", "revision": "doc_revision:original",
                    "nodes_count": 3, "chunks": {"total": 2, "embedded": 2}}
        nodes = [dict(base, id="doc_node:" + role, role=role, revision="doc_revision:original", span=span,
                      embedding_model=c.partition_model(self.space)) for role, span in
                 (("document", rendered["root_span"]), ("paragraph", rendered["paragraph_spans"][0]),
                  ("chunk", rendered["chunk_spans"][0]))]
        return [driver.primary, source, accepted, doc, record, pipeline, nodes, self.space, rendered]

    def test_publication_rejects_missing_embedding_and_foreign_partition(self):
        values = self.publication()
        c.retrieval_publication(*values)
        for change in (lambda values: values[5]["chunks"].update(embedded=1),
                       lambda values: values[6][1].update(embedding_model="fake:384"),
                       lambda values: values[6][2].update(revision="doc_revision:latest")):
            altered = self.publication()
            change(altered)
            with self.assertRaises(ValueError):
                c.retrieval_publication(*altered)

    def test_actual_document_wire_preserves_frozen_root_span(self):
        # Independent wire authority: types/records/doc_node.rs NodeRole::Document
        # serializes as document; evidence_work_kinds creates that exact role.
        # The D rendered map's coordinate name remains root_span.
        values = self.publication()
        self.assertEqual([row["role"] for row in values[6]], ["document", "paragraph", "chunk"])
        self.assertEqual(values[6][0]["span"], values[8]["root_span"])
        plan = driver.plan()
        for authority in ("crates/types/src/records/doc_node.rs",
                          "crates/pipeline/src/scheduler/evidence_work_kinds.rs"):
            self.assertEqual(plan["input_sha256"][authority], driver.core.file_digest(driver.ROOT / authority))
        c.retrieval_publication(*values)

    def test_publication_rejects_root_missing_duplicate_and_short_spans(self):
        for change in (lambda values: values[6][0].update(role="root"),
                       lambda values: values[6].pop(0),
                       lambda values: values[6][1].update(role="document"),
                       lambda values: values[6][0]["span"].update(end=values[6][0]["span"]["end"] - 1)):
            values = self.publication()
            # Keep the independent map fixed while mutating only observed data.
            values[6] = copy.deepcopy(values[6])
            change(values)
            with self.assertRaises(ValueError):
                c.retrieval_publication(*values)

    def test_disabled_manifest_is_rejected_before_plan_or_process(self):
        raw = driver.core.encoded({"format": "recall-installed-local-run/v1", "execution_authorized": False})
        with patch.object(driver.core.process_capture, "read_bounded", return_value=raw), patch.object(driver, "plan") as plan:
            with self.assertRaises(ValueError):
                driver.validate_manifest(Path("/inert"), driver.core.digest(raw))
        plan.assert_not_called()

    def test_static_runtime_refuses_conversion_old_api_and_other_artifact(self):
        manifest = {"source_commit": "a" * 40, "archive_sha256": "b" * 64, "binary_sha256": "c" * 64}
        receipt = {**manifest, "format": "recall-installed-local-static-runtime/v1", "reviewer": "lifecycle",
            "reviewed": True, "actual_build": True, "link_mode": "static", "ort_version": "2.0.0-rc.12",
            "api": 24, "load_dynamic": False, "ort_features": ["api-24", "download-binaries"],
            "static_link_directive": "cargo:rustc-link-lib=static=onnxruntime",
            "feature_fingerprint_sha256": "d" * 64, "link_output_sha256": "e" * 64,
            "distribution_sha256": "f" * 64,
            "original_build_receipt": {"path": "/inputs/runtime-evidence/build.json", "sha256": "a" * 64},
            "feature_receipts": [{"path": "/inputs/runtime-evidence/ort.json", "sha256": "d" * 64},
                                 {"path": "/inputs/runtime-evidence/ort-sys.json", "sha256": "a" * 64}],
            "link_output": {"path": "/inputs/runtime-evidence/link-output", "sha256": "e" * 64},
            "distribution": {"url": "https://example.invalid/immutable-static-dist", "sha256": "f" * 64},
            "static_library": {"bytes": 128, "sha256": "b" * 64}}
        c.static_runtime(manifest, receipt)
        for field, value in (("ort_version", "1.20.1"), ("api", 20), ("actual_build", False),
                             ("load_dynamic", True), ("binary_sha256", "f" * 64),
                             ("link_output_sha256", ""), ("ort_features", ["api-24", "load-dynamic"]),
                             ("original_build_receipt", None), ("feature_receipts", []),
                             ("static_library", {"bytes": 0, "sha256": "b" * 64})):
            with self.assertRaises(ValueError):
                c.static_runtime(manifest, dict(receipt, **{field: value}))

    def test_original_clock_cannot_restart_expired_allocation(self):
        now = {"mono_before_ns": 10, "wall_ns": 10, "mono_after_ns": 11}
        manifest = {"expires_at": "1970-01-01T00:00:01Z", "original_allocation_clock": now}
        with patch.object(driver.core.process_capture.time, "monotonic_ns", return_value=9000000000):
            _, deadline = driver.original_clock(manifest)
        self.assertEqual(deadline, 1000000000)
        # Actual work uses that original deadline, not the current timestamp.
        obj = object.__new__(driver.LocalDriver)
        obj.cases = {"limits": c.LIMITS}
        obj.work_deadline = 1
        obj.manifest = {"expires_at": "2100-01-01T00:00:00Z"}
        obj.started, obj.allocation_deadline = 0, 1800
        with patch.object(driver.time, "monotonic", return_value=1801), self.assertRaises(ValueError):
            obj.remaining()

    def test_readonly_parent_does_not_hide_writable_nested_model_mount(self):
        mountinfo = "1 0 0:1 / / ro - rootfs rootfs ro\n2 1 0:2 / /model-assets/version rw - tmpfs tmpfs rw\n"
        with patch.object(Path, "resolve", lambda path, **kwargs: path), patch.object(Path, "read_text", return_value=mountinfo):
            driver.readonly_mount(Path("/model-assets"))
            with self.assertRaises(ValueError):
                driver.readonly_mount(Path("/model-assets/version/model.onnx"))

    def test_terminal_cache_failure_keeps_original_cleanup_and_capture(self):
        obj = object.__new__(driver.LocalDriver)
        obj.report, obj.cache_before, obj.process_journal = {}, {}, object()
        obj.child, obj.owned_commands = None, []
        obj.started, obj.requests, obj.serial, obj.polls = 0, 0, 0, 0
        obj.publications, obj.query_rows, obj.root = [], {}, Path("/unused")
        phases = []
        obj.begin_process_capture = lambda: None
        obj.execute = lambda: None
        obj.stop = lambda **kwargs: phases.append("stop")
        def fail_cache(_phase):
            phases.append("cache")
            raise ValueError("cache changed")
        obj.assets = fail_cache
        obj.finish_process_capture = lambda: phases.append("capture")
        obj.cleanup_remaining, obj.reserve_capture = lambda: 40, lambda _amount: None
        with patch.object(driver.core, "private_new"):
            self.assertEqual(obj.run(), 1)
        self.assertEqual(phases, ["stop", "cache", "capture"])
        self.assertFalse(obj.report["owned_cleanup_complete"])
        self.assertFalse(obj.report["product_acceptance"])


    def _publication_head_fixture(self, *, replacement=False, mutate_head=None,
                                  operation_states=("complete",), mutate_operation=None):
        # Exercise actual publish/source-request/acceptance/original-operation
        # methods. Only the HTTP peer is simulated; stop before evidence reads.
        source = {"space": self.space["id"], "id": "route/å %", "revision": "r1",
                  "text": "Original synthetic source evidence."}
        obj = object.__new__(driver.LocalDriver)
        obj.corpus = {"spaces": [self.space]}
        obj.manifest = {"nonce": "native-head-route-control", "fixture_timestamp": "2026-10-09T00:00:00Z"}
        obj.cases = {"limits": c.LIMITS}
        obj.polls, obj.requests, obj.report, obj.publications = 0, 0, {}, []
        obj.remaining = lambda: 30
        expected_head = "/v1/spaces/semantics-household/sources/route%2F%C3%A5%20%25/head"
        self.assertEqual(self.space["id"], "semantics-household")
        request = driver.primary.source_request(source, "surreal-db", obj.manifest["nonce"],
            obj.manifest["fixture_timestamp"], c.security(self.space), "g0" if replacement else None)
        original = {"operation_id": "operation-new", "generation": "g1",
            "internal_source_id": request["source"]["id"], "desired_revision": "r1",
            "doc_id": request["doc"]["id"], "work_item_id": "work_item:new"}
        previous = dict(original, operation_id="operation-old", generation="g0", desired_revision="r0")
        key = (source["space"], source["id"])
        obj.current = {key: previous} if replacement else {}
        calls, posted = [], []
        observed_operations = []
        obj.observed_operations = observed_operations

        def completed(value):
            return dict(value, lifecycle="active", state="complete",
                        published_revision=value["desired_revision"], error_code=None)

        def json_peer(method, path, space, body=None, *, status=200):
            obj.requests += 1
            calls.append((method, path, space))
            self.assertEqual(space, source["space"])
            if (method, path) == ("POST", "/v1/sources/ingest"):
                self.assertEqual(status, 202)
                self.assertEqual(body["intake"]["expected_generation"], "g0" if replacement else None)
                self.assertEqual(body["ingest_mode"], "retrieval")
                self.assertEqual(body["source"]["external_id"], source["id"])
                self.assertEqual(body["doc"]["text"], source["text"])
                posted.append(copy.deepcopy(body))
                return {"source_id": original["internal_source_id"], "doc_id": original["doc_id"],
                    "work_item_id": original["work_item_id"], "operation": {
                        "space_id": source["space"], "desired_revision": source["revision"],
                        "operation_id": original["operation_id"], "source_generation": original["generation"],
                        "source_id": original["internal_source_id"]}}
            if (method, path) == ("GET", "/v1/operations/operation-new"):
                self.assertEqual(len(posted), 1)
                index = len(observed_operations)
                state = operation_states[min(index, len(operation_states) - 1)]
                value = completed(original)
                value.update(state=state,
                    published_revision=original["desired_revision"] if state == "complete" else None,
                    error_code="transient_failure" if state == "failed_retryable" else
                               "final_failure" if state == "failed_final" else None)
                if mutate_operation is not None:
                    mutate_operation(index, value)
                observed_operations.append(copy.deepcopy(value))
                return value
            if (method, path) == ("GET", expected_head):
                phase = "current" if posted else "predecessor"
                self.assertTrue(posted or replacement)
                value = completed(original if posted else previous)
                if mutate_head is not None:
                    mutate_head(phase, value)
                return value
            if method == "GET" and path.startswith("/v1/docs/"):
                raise RuntimeError("stop before evidence reads")
            self.fail("unexpected publication request: " + method + " " + path)

        obj.json = json_peer
        return obj, source, expected_head, calls, posted

    def test_native_publication_reads_head_route_before_evidence(self):
        obj, source, expected_head, calls, posted = self._publication_head_fixture()
        with self.assertRaisesRegex(RuntimeError, "stop before evidence reads"):
            obj.publish(source, driver.time.monotonic() + 30)
        self.assertEqual(calls[2], ("GET", expected_head, source["space"]))
        self.assertEqual(sum(path == expected_head for _, path, _ in calls), 1)
        self.assertEqual(len(posted), 1)
        self.assertEqual(obj.polls, 1)
        self.assertEqual(obj.current, {})
        self.assertEqual(obj.publications, [])
        self.assertEqual(obj.source_path(source["space"], source["id"]), expected_head[:-5])

    def test_native_replacement_checks_head_before_and_after_ingest(self):
        obj, source, expected_head, calls, posted = self._publication_head_fixture(replacement=True)
        previous = copy.deepcopy(obj.current)
        with self.assertRaisesRegex(RuntimeError, "stop before evidence reads"):
            obj.publish(source, driver.time.monotonic() + 30)
        self.assertEqual(calls[0], ("GET", expected_head, source["space"]))
        self.assertEqual(calls[3], ("GET", expected_head, source["space"]))
        self.assertEqual(len(posted), 1)
        self.assertEqual(obj.current, previous)
        self.assertEqual(obj.publications, [])

    def test_native_head_refuses_changed_original_identity(self):
        for field, wrong in (("operation_id", "other-operation"), ("generation", "g-other"),
                ("internal_source_id", "source:other"), ("desired_revision", "other-revision"),
                ("published_revision", "old-revision"), ("lifecycle", "erased"),
                ("state", "running"), ("error_code", "not_ready")):
            with self.subTest(field=field):
                obj, source, expected_head, calls, posted = self._publication_head_fixture(
                    mutate_head=lambda _phase, value: value.update({field: wrong}))
                with self.assertRaises(ValueError):
                    obj.publish(source, driver.time.monotonic() + 30)
                self.assertEqual(calls[-1], ("GET", expected_head, source["space"]))
                self.assertEqual(len(posted), 1)
                self.assertEqual(obj.publications, [])
                self.assertEqual(obj.current, {})

    def test_native_predecessor_head_refuses_change_before_ingest(self):
        obj, source, expected_head, calls, posted = self._publication_head_fixture(replacement=True,
            mutate_head=lambda _phase, value: value.update(generation="foreign-generation"))
        previous = copy.deepcopy(obj.current)
        with self.assertRaises(ValueError):
            obj.publish(source, driver.time.monotonic() + 30)
        self.assertEqual(calls, [("GET", expected_head, source["space"])])
        self.assertEqual(posted, [])
        self.assertEqual(obj.current, previous)
        self.assertEqual(obj.publications, [])

    def _observe_publication(self, obj, source, deadline):
        # Only time and the fixture's HTTP peer are simulated. Run the actual
        # publish loop, original identity checks, caps and completed-head check.
        clock = [0.0]
        sleeps = []

        def sleep(seconds):
            self.assertGreater(seconds, 0)
            sleeps.append(seconds)
            clock[0] += seconds

        with patch.object(driver.time, "monotonic", side_effect=lambda: clock[0]), \
             patch.object(driver.time, "sleep", side_effect=sleep):
            try:
                obj.publish(source, deadline)
            finally:
                obj.observed_sleeps = sleeps
                obj.observed_clock = clock[0]

    def test_retryable_original_continues_to_completed_head_without_resubmission(self):
        states = ("running", "failed_retryable", "running", "complete")
        obj, source, head, calls, posted = self._publication_head_fixture(operation_states=states)
        with self.assertRaisesRegex(RuntimeError, "stop before evidence reads"):
            self._observe_publication(obj, source, 30)
        self.assertEqual([row["state"] for row in obj.observed_operations], list(states))
        self.assertEqual(obj.polls, 4)
        self.assertEqual(obj.observed_sleeps, [c.LIMITS["poll_seconds"]] * 3)
        self.assertEqual(len(posted), 1)
        self.assertEqual(calls[-2], ("GET", head, source["space"]))
        self.assertEqual({row["operation_id"] for row in obj.observed_operations}, {"operation-new"})
        self.assertEqual(obj.current, {})
        self.assertEqual(obj.publications, [])

    def test_retryable_then_final_failure_stops_before_head_or_evidence(self):
        obj, source, head, calls, posted = self._publication_head_fixture(
            operation_states=("failed_retryable", "failed_final"))
        with self.assertRaisesRegex(ValueError, "original local publication failed"):
            self._observe_publication(obj, source, 30)
        self.assertEqual(obj.polls, 2)
        self.assertEqual(obj.observed_sleeps, [c.LIMITS["poll_seconds"]])
        self.assertEqual(len(posted), 1)
        self.assertTrue(all(path != head and not path.startswith("/v1/docs/") for _, path, _ in calls))
        self.assertEqual(obj.publications, [])

    def test_retryable_receipts_do_not_renew_original_publication_deadline(self):
        obj, source, head, calls, posted = self._publication_head_fixture(
            operation_states=("failed_retryable",))
        with self.assertRaisesRegex(ValueError, "original publication deadline reached"):
            self._observe_publication(obj, source, 3)
        self.assertEqual(obj.observed_clock, 3)
        self.assertEqual(obj.observed_sleeps, [2, 1])
        self.assertEqual(obj.polls, 2)
        self.assertEqual(len(posted), 1)
        self.assertFalse(any(path == head for _, path, _ in calls))
        self.assertEqual(obj.publications, [])

    def test_retryable_receipts_preserve_global_poll_cap(self):
        obj, source, head, calls, posted = self._publication_head_fixture(
            operation_states=("failed_retryable",))
        obj.polls = c.LIMITS["publication_polls"] - 1
        with self.assertRaisesRegex(ValueError, "original publication poll cap reached"):
            self._observe_publication(obj, source, 30)
        self.assertEqual(len(obj.observed_operations), 1)
        self.assertEqual(obj.polls, c.LIMITS["publication_polls"] + 1)
        self.assertEqual(obj.observed_sleeps, [c.LIMITS["poll_seconds"]])
        self.assertEqual(len(posted), 1)
        self.assertFalse(any(path == head for _, path, _ in calls))
        self.assertEqual(obj.publications, [])

    def test_retryable_receipt_cannot_change_original_authority(self):
        for field, wrong in (("operation_id", "other"), ("generation", "g2"),
                ("internal_source_id", "source:other"), ("desired_revision", "r2"),
                ("lifecycle", "erased"), ("state", "unknown")):
            with self.subTest(field=field):
                obj, source, head, calls, posted = self._publication_head_fixture(
                    operation_states=("failed_retryable",),
                    mutate_operation=lambda _index, value: value.update({field: wrong}))
                with self.assertRaises(ValueError):
                    self._observe_publication(obj, source, 30)
                self.assertEqual(obj.polls, 1)
                self.assertEqual(obj.observed_sleeps, [])
                self.assertEqual(len(posted), 1)
                self.assertFalse(any(path == head for _, path, _ in calls))
                self.assertEqual(obj.publications, [])

    def test_completion_after_retry_still_requires_exact_revision_and_clear_error(self):
        for field, wrong in (("published_revision", "r0"), ("error_code", "transient_failure")):
            with self.subTest(field=field):
                def mutate(index, value):
                    if index == 1:
                        value[field] = wrong
                obj, source, head, calls, posted = self._publication_head_fixture(
                    operation_states=("failed_retryable", "complete"), mutate_operation=mutate)
                with self.assertRaisesRegex(ValueError, "original publication has not completed"):
                    self._observe_publication(obj, source, 30)
                self.assertEqual(obj.polls, 2)
                self.assertEqual(len(posted), 1)
                self.assertFalse(any(path == head for _, path, _ in calls))
                self.assertEqual(obj.publications, [])


if __name__ == "__main__":
    unittest.main()
