"""Offline contract/transport controls; no candidate process or TCP connection."""
from __future__ import annotations

import asyncio
import base64
import copy
import io
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import runtime_package_bootstrap as driver
import runtime_package_bootstrap_contract as contract
import runtime_package_bootstrap_io as files


def space(name, policy, level):
    return {"name": name, "security": {"policy_id": policy, "level": level, "labels": [], "handling": []},
            "embedding_provider": "openai", "embedding_model": "reviewed-model", "embedding_dims": 8,
            "purpose": "independently frozen extraction guidance", "skill": None,
            "processing": {"generation": 1, "index_generation": 0, "policy_id": policy,
                "connections": {}, "profiles": {}, "bindings": {"embedding": {"profile_id": "reviewed"}},
                "active_embedding_profile_id": "reviewed", "pending_reindex": None}}


def plan():
    nonce = "original-allocation-1234"
    return {"format": "recall-runtime-bootstrap-plan/v1", "source_commit": "a" * 40,
            "artifact_sha256": "b" * 64, "paired_artifact_sha256": "c" * 64, "runtime_sha256": "d" * 64,
            "backend": "elephant-db", "endpoint": "http://127.0.0.1:43130", "run_nonce": nonce,
            "expires_at": "2050-01-01T00:00:00Z", "spaces": [space("household", "household-policy", 3),
                                                                 space("business", "business-policy", 7)],
            "service_scopes": sorted(contract.CORE_SCOPES), "dependencies": {path: "e" * 64 for path in files.DEPENDENCIES},
            "run_directory": "/work/bootstrap-" + nonce + "-elephant-db"}


def configuration(request):
    return {"id": "space_config:11111111-1111-4111-8111-111111111111", "space_id": request["name"],
            "created_at": "2026-10-07T01:00:00Z", "updated_at": "2026-10-07T01:00:00Z",
            "created_by": "primary-bootstrap-" + request["name"], "subjects": [], "subjects_state": "complete",
            "provenance": {"sources": [], "chain": []}, **{k: v for k, v in request.items() if k not in ("name", "skill")}}


def receipt(value):
    root = value["run_directory"]
    configs = {row["name"]: configuration(row) for row in value["spaces"]}
    return {"format": contract.FORMAT, "status": "bootstrapped", "qualification_scope": contract.SCOPE,
            "qualified": False, "product_acceptance": False,
            **{key: value[key] for key in contract.IDENTITIES if key != "configuration_sha256"},
            "configuration_sha256": "f" * 64, "authentication_enabled": True, "clean_persistent_store": True,
            "schema_explicitly_initialized": True, "schema_operation": {"kind": "initialize_runtime", "capture_index": 1, "process_index": None},
            "launch_context_sha256": "1" * 64, "isolation_sha256": "2" * 64, "plan_sha256": contract.sha(contract.encode(value)),
            "runtime_config_sha256": "3" * 64, "space_requests_sha256": contract.sha(contract.encode(value["spaces"])),
            "observed_configurations_sha256": contract.sha(contract.encode(configs)),
            "allocation_started_monotonic_ns": 1_000_000_000, "started_monotonic_ns": 2_000_000_000,
            "observed_monotonic_ns": 3_000_000_000, "original_deadline_monotonic_ns": 100_000_000_000,
            "whole_deadline_monotonic_ns": 500_000_000_000,
            "bootstrap_process": {"pid": 30, "ppid": 20, "pgrp": 20, "session": 20, "start_ticks": 300},
            "processes": {"recall": {"pid": 40, "ppid": 30, "process_group": 40, "session": 40, "start_ticks": 400,
                "executable": "/opt/recall/bin/elephant", "executable_sha256": value["artifact_sha256"]}},
            "process_commands": {"path": root + "/process-commands.json", "sha256": "4" * 64},
            "checks": {name: [1] for name in contract.CHECKS}, "captures": [{"request_index": 1,
                "request_path": root + "/captures/001.request.json", "response_path": root + "/captures/001.response.json",
                "request_sha256": "5" * 64, "response_sha256": "6" * 64}], "configurations": configs,
            "token_files": {name: root + "/private/service-" + name + ".token" for name in configs}}


class Controls(unittest.TestCase):
    def setUp(self):
        self.process_guard = mock.patch("subprocess.Popen", side_effect=AssertionError("process forbidden"))
        self.process_guard.start()
        self.tcp_guard = mock.patch("socket.create_connection", side_effect=AssertionError("TCP forbidden"))
        self.tcp_guard.start()

    def tearDown(self):
        self.tcp_guard.stop()
        self.process_guard.stop()

    def test_d28_launch_forwards_complete_original_static_anchor_without_future_hashes(self):
        hash_fields = {"manifest_sha256", "proposal_sha256", "corpus_sha256", "artifact_sha256",
            "paired_artifact_sha256", "runtime_sha256", "configuration_sha256", "gateway_sha256",
            "bindings_sha256", "bridge_sha256", "context_helper_sha256", "command_sha256"}
        gateway = SimpleNamespace(HASH_FIELDS=hash_fields, ANCHOR_FORMAT="recall-gateway-launch-anchor/v1")
        expected = {name: "e" * 64 for name in hash_fields - {"manifest_sha256", "configuration_sha256"}}
        expected.update(format=gateway.ANCHOR_FORMAT, run_nonce="original-allocation-1234", source_commit="a" * 40,
                        allocation_sha256=contract.D28_ALLOCATION_SHA256,
                        caps={"physical_dispatches": 2100, "maximum_nano_usd": 3_000_000_000})
        self.assertEqual(len(expected), 15)
        before = copy.deepcopy(expected)
        self.assertIs(driver.launch_expectations(gateway, expected, contract.D28_ALLOCATION_SHA256), expected)
        self.assertEqual(expected, before)
        # Exercise the active context caller with the original G reader mocked;
        # no file, process, namespace or network observations are performed.
        original_plan = plan()
        phase_expected = {**expected, **{key: original_plan[key] for key in
            ("source_commit", "artifact_sha256", "paired_artifact_sha256", "runtime_sha256", "run_nonce")}}
        context = {"anchor": {"input_inventory_sha256": "f" * 64}}
        raw_context = contract.encode(context)
        gateway.read_context = mock.Mock(return_value=context)
        def actual_hash(path):
            return (original_plan["artifact_sha256"] if path == driver.BINARY else
                    original_plan["paired_artifact_sha256"] if path == driver.PAIRED else "e" * 64)
        with mock.patch.object(driver, "ROOT", Path("/opt/recall")), \
             mock.patch.object(driver, "file_hash", side_effect=actual_hash), \
             mock.patch.object(driver, "read_bound", return_value=raw_context), \
             mock.patch.object(driver, "reviewed_module", return_value=gateway) as load_gateway, \
             mock.patch.dict(driver.os.environ, {"RECALL_GATEWAY_CONTEXT_PATH": "/work/context.json",
                                                 "RECALL_GATEWAY_CONTEXT_SHA256": contract.sha(raw_context)}):
            observed = driver.actual_context({"plan": original_plan, "review": {"expected_launch": phase_expected}},
                                             allocation_sha256=contract.D28_ALLOCATION_SHA256)
        self.assertIs(observed[0], gateway)
        self.assertEqual(observed[2], raw_context)
        self.assertIs(gateway.read_context.call_args.kwargs["expected"], phase_expected)
        self.assertTrue(gateway.read_context.call_args.kwargs["immediate_parent"])
        load_gateway.assert_called_once_with("bootstrap_actual_gateway_context",
            Path("/opt/recall/tests/recall_preview_semantics/gateway/launch_context.py"), "e" * 64)
        for key in expected:
            missing = {name: value for name, value in expected.items() if name != key}
            with self.subTest(missing=key), self.assertRaises(ValueError):
                driver.launch_expectations(gateway, missing, contract.D28_ALLOCATION_SHA256)
        for key in ("manifest_sha256", "configuration_sha256", "input_inventory_sha256", "clock", "parent"):
            with self.subTest(future=key), self.assertRaises(ValueError):
                driver.launch_expectations(gateway, {**expected, key: "future"}, contract.D28_ALLOCATION_SHA256)
        for value in ({"physical_dispatches": 2101, "maximum_nano_usd": 3_000_000_000},
                      {"physical_dispatches": 2100.0, "maximum_nano_usd": 3_000_000_000}):
            with self.subTest(caps=value), self.assertRaises(ValueError):
                driver.launch_expectations(gateway, {**expected, "caps": value}, contract.D28_ALLOCATION_SHA256)
        with self.assertRaises(ValueError):
            driver.launch_expectations(gateway, {**expected, "format": "other"}, contract.D28_ALLOCATION_SHA256)
        primary = {key: value for key, value in expected.items() if key not in ("format", "caps")}
        primary["allocation_sha256"] = None
        self.assertIs(driver.launch_expectations(gateway, primary, None), primary)
        for key in ("format", "caps"):
            with self.subTest(primary_extra=key), self.assertRaises(ValueError):
                driver.launch_expectations(gateway, {**primary, key: expected[key]}, None)

    def test_reviewed_source_executes_original_hashed_bytes_without_loader_reread(self):
        raw = b'observed = "original reviewed source"\n'
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary).resolve() / "reviewed_helper.py"
            path.write_bytes(raw)
            original_reader = files.read_bound
            def replace_after_read(selected, maximum):
                observed = original_reader(selected, maximum)
                path.write_bytes(b'observed = "replacement must never execute"\n')
                return observed
            with mock.patch.object(files, "read_bound", side_effect=replace_after_read) as reader, \
                 mock.patch("importlib.machinery.SourceFileLoader.exec_module",
                            side_effect=AssertionError("second source or bytecode read forbidden")):
                module = files.reviewed_module("original_reviewed_helper", path, contract.sha(raw))
            reader.assert_called_once_with(path, 8 * 1024**2)
            self.assertEqual(module.observed, "original reviewed source")
            self.assertEqual(module.__name__, "original_reviewed_helper")
            self.assertEqual(module.__file__, str(path))
            self.assertEqual(module.__spec__.origin, str(path))
            with self.assertRaisesRegex(ValueError, "helper source differs"):
                files.reviewed_module("changed_reviewed_helper", path, contract.sha(raw))

    def test_actual_worker_recipe_is_required_and_replacement_does_not_rewrite_original(self):
        recipe = {"format": "recall-managed-worker-recipe/v1", "backend": "elephant-db",
                  "instance_id": "11111111-1111-4111-8111-111111111111",
                  "work_queue": {"transient_retry_max_attempts": 7}, "embed_worker": {"batch_size": 9}}
        instance = object.__new__(driver.Bootstrap)
        instance.plan = plan()
        instance.worker_recipe_observation = None
        instance.http = SimpleNamespace(tool=mock.AsyncMock(return_value=({"managed_worker_recipe": recipe}, 41)))
        original = asyncio.run(instance.observe_worker_recipe())
        instance.http.tool.assert_awaited_once_with("get_runtime_config", {})
        self.assertEqual(original["recipe_sha256"], contract.sha(contract.encode(recipe)))
        instance.http.tool.return_value = ({"managed_worker_recipe": recipe}, 42)
        self.assertEqual(asyncio.run(instance.observe_worker_recipe())["request_index"], 42)
        retained = copy.deepcopy(instance.worker_recipe_observation)
        instance.http.tool.return_value = ({"managed_worker_recipe": {**recipe, "instance_id": "22222222-2222-4222-8222-222222222222"}}, 43)
        with self.assertRaises(ValueError):
            asyncio.run(instance.observe_worker_recipe())
        self.assertEqual(instance.worker_recipe_observation, retained)
        for value in ({}, {"managed_worker_recipe": None}, {"managed_worker_recipe": {**recipe, "backend": "surreal-db"}}):
            instance.http.tool.return_value = (value, 44)
            with self.assertRaises(ValueError):
                asyncio.run(instance.observe_worker_recipe())

    def test_actual_embedding_recipe_tracks_each_original_named_binding(self):
        instance = object.__new__(driver.Bootstrap)
        instance.plan = plan()
        instance.embedding_recipe_observations = {}
        instance.configurations = {}
        responses = []
        for index, request in enumerate(instance.plan["spaces"]):
            name = request["name"]
            config = configuration(request)
            processing = config["processing"]
            processing["bindings"] = {operation: {"id": operation + "-original", "profile_id": "reviewed"}
                                      for operation in ("query_embedding", "document_embedding")}
            processing["profiles"] = {"reviewed": {"id": "reviewed", "preprocessing": "text-v1"}}
            instance.configurations[name] = config
            recipes = {operation: {
                "provenance": {"config": {"space_id": name, "config_id": config["id"],
                    "generation": processing["generation"], "policy_id": processing["policy_id"]},
                    "operation": operation, "binding_id": binding["id"], "profile_id": "reviewed"},
                "partition": {"space_id": name, "profile_id": "reviewed", "preprocessing": "text-v1"},
                "max_total_http_attempts": 3,
            } for operation, binding in processing["bindings"].items()}
            responses.append({"request_index": index + 80, "body": {"space": name,
                "processing_generation": 1, "embedding_execution_recipes": recipes}})
        instance.http = SimpleNamespace(request=mock.AsyncMock(side_effect=copy.deepcopy(responses)),
                                        success=lambda response: response["body"])
        original = asyncio.run(instance.observe_embedding_recipes())
        self.assertEqual(set(original), {"household", "business"})
        self.assertEqual(instance.http.request.await_args_list[0], mock.call(
            "GET", "/v1/spaces/household/status", role="service-household", space="household"))
        for alteration in ("attempts", "foreign", "absent", "generation", "binding"):
            changed = copy.deepcopy(responses)
            body = changed[1]["body"]
            recipe = body["embedding_execution_recipes"]["query_embedding"]
            if alteration == "attempts":
                recipe["max_total_http_attempts"] = 4
            elif alteration == "foreign":
                recipe["partition"]["space_id"] = "household"
            elif alteration == "absent":
                del body["embedding_execution_recipes"]
            elif alteration == "generation":
                body["processing_generation"] = 2
            else:
                recipe["provenance"]["binding_id"] = "replacement"
            instance.http.request.side_effect = changed
            with self.assertRaises(ValueError):
                asyncio.run(instance.observe_embedding_recipes())
            self.assertEqual(instance.embedding_recipe_observations, original)

    def test_inert_default_never_reads_private_inputs_or_credentials(self):
        with mock.patch("sys.argv", ["runtime_package_bootstrap.py"]), mock.patch("sys.stdout", new_callable=io.StringIO) as output, \
             mock.patch.object(driver, "phase_inputs", side_effect=AssertionError("input read forbidden")), \
             mock.patch.object(driver, "actual_context", side_effect=AssertionError("context access forbidden")):
            self.assertEqual(driver.main(), 0)
            value = contract.strict_json(output.getvalue().encode())
        self.assertFalse(value["execution_authorized"])
        self.assertFalse(value["qualified"])
        self.assertFalse(value["product_acceptance"])

    def test_plan_is_acyclic_and_preserves_named_classification_and_scopes(self):
        value = plan()
        contract.validate_plan(value)
        for key in ("configuration_sha256", "review", "primary_manifest_template", "input_inventory_sha256"):
            with self.subTest(key=key), self.assertRaises(ValueError):
                contract.validate_plan({**value, key: "future"})
        for mutate in (lambda p: p["spaces"].pop(), lambda p: p["spaces"][1]["security"].update(level=0),
                       lambda p: p["service_scopes"].remove("write:doc"),
                       lambda p: p["service_scopes"].append("*"), lambda p: p.update(endpoint="http://host:3000"),
                       lambda p: p.update(run_directory="/work/../host")):
            bad = copy.deepcopy(value)
            mutate(bad)
            with self.assertRaises(ValueError):
                contract.validate_plan(bad)

    def test_actual_generated_envelope_is_checked_without_inventing_ids(self):
        request = plan()["spaces"][0]
        actual = configuration(request)
        contract.validate_config(request, actual, "primary-bootstrap-household")
        actual["id"] = "space_config:22222222-2222-4222-8222-222222222222"
        contract.validate_config(request, actual, "primary-bootstrap-household")
        for mutate in (lambda a: a["security"].update(level=0), lambda a: a.update(created_by="global-operator"),
                       lambda a: a.update(updated_at="2026-10-07T02:00:00Z"), lambda a: a.update(purpose="changed"),
                       lambda a: a.update(subjects_state="unknown"), lambda a: a.update(deleted_at="2026-10-07T02:00:00Z")):
            bad = copy.deepcopy(actual)
            mutate(bad)
            with self.assertRaises(ValueError):
                contract.validate_config(request, bad, "primary-bootstrap-household")

    def test_bootstrap_observation_never_claims_qualification_or_omits_proofs(self):
        value = plan()
        observed = receipt(value)
        contract.validate_receipt(observed, value)
        for mutate in (lambda r: r.update(qualified=True), lambda r: r.update(product_acceptance=True),
                       lambda r: r["checks"].pop("grant_revocation"), lambda r: r["checks"].update(wrong_space=[]),
                       lambda r: r["checks"].update(ready=[999]), lambda r: r.update(captures=[]),
                       lambda r: r.update(processes={}), lambda r: r["processes"]["recall"].update(ppid=99),
                       lambda r: r["processes"]["recall"].update(executable_sha256="0" * 64)):
            bad = copy.deepcopy(observed)
            mutate(bad)
            with self.assertRaises(ValueError):
                contract.validate_receipt(bad, value)

    def test_receipt_preserves_observed_configuration_and_actual_schema_reference(self):
        value = plan()
        observed = receipt(value)
        for mutate in (lambda r: r["configurations"]["household"].update(id="substituted"),
                       lambda r: r["schema_operation"].update(capture_index=None),
                       lambda r: r["schema_operation"].update(kind="migrate"),
                       lambda r: r["token_files"].update(household="/work/another.token")):
            bad = copy.deepcopy(observed)
            mutate(bad)
            with self.assertRaises(ValueError):
                contract.validate_receipt(bad, value)

    def test_original_clock_cannot_reset_or_exceed_whole_allocation(self):
        value, observed = plan(), receipt(plan())
        for mutate in (lambda r: r.update(original_deadline_monotonic_ns=900_000_000_000),
                       lambda r: r.update(allocation_started_monotonic_ns=4_000_000_000),
                       lambda r: r.update(observed_monotonic_ns=100_000_000_000),
                       lambda r: r.update(started_monotonic_ns=True)):
            bad = copy.deepcopy(observed)
            mutate(bad)
            with self.assertRaises(ValueError):
                contract.validate_receipt(bad, value)
        with mock.patch.object(files.time, "monotonic_ns", return_value=10_000_000_000):
            allocation = files.Allocation(1_000_000_000, 200_000_000_000, 500_000_000_000, "2050-01-01T00:00:00Z")
            self.assertEqual(allocation.remaining(), 115)
            allocation.bootstrap = False
            self.assertEqual(allocation.remaining(), 115)
            self.assertEqual(allocation.remaining(cleanup=True), 190)
            self.assertEqual(allocation.deadline_ns, 200_000_000_000)
        with mock.patch.object(files.time, "monotonic_ns", return_value=200_000_000_000), self.assertRaises(ValueError):
            allocation.remaining(cleanup=True)

    def test_d28_clock_is_explicit_and_never_extends_original_whole_deadline(self):
        start, deadline = 1_000_000_000, 7_000_000_000_000
        with mock.patch.object(files.time, "monotonic_ns", return_value=start):
            with self.assertRaises(ValueError):
                files.Allocation(start, deadline, deadline, "2050-01-01T00:00:00Z")
            allocation = files.Allocation(start, deadline, deadline, "2050-01-01T00:00:00Z",
                                          maximum_seconds=contract.D28_WHOLE_SECONDS)
            self.assertEqual(allocation.remaining(), 300)
            allocation.bootstrap = False
            self.assertEqual(allocation.remaining(), 6924)
            self.assertEqual(allocation.deadline_ns, deadline)
            for limit in (True, 2701, 7801):
                with self.assertRaises(ValueError):
                    files.Allocation(start, deadline, deadline, "2050-01-01T00:00:00Z", maximum_seconds=limit)
            with self.assertRaises(ValueError):
                files.Allocation(start, deadline + 1, deadline, "2050-01-01T00:00:00Z",
                                 maximum_seconds=contract.D28_WHOLE_SECONDS)
        observed = receipt(plan())
        observed.update(original_deadline_monotonic_ns=deadline, whole_deadline_monotonic_ns=deadline)
        with self.assertRaises(ValueError):
            contract.validate_receipt(observed, plan())
        contract.validate_receipt(observed, plan(), maximum_backend_seconds=contract.D28_WHOLE_SECONDS)
        with mock.patch.object(files.time, "monotonic_ns", return_value=deadline), self.assertRaises(ValueError):
            allocation.remaining(cleanup=True)

    def test_ordinary_token_covers_original_allocation_but_management_remains_short_lived(self):
        self.assertEqual(contract.token_lifetime(7000, management=False), 7000)
        self.assertEqual(contract.token_lifetime(7000, management=True), 3600)
        self.assertEqual(contract.token_lifetime(2699.2, management=False), 2700)
        self.assertEqual(contract.token_lifetime(10**8, management=False), 31 * 24 * 3600)
        for value in (0, -1, True, float("inf"), float("nan")):
            with self.assertRaises(ValueError):
                contract.token_lifetime(value, management=False)

    def test_foreign_or_missing_d28_launch_allocation_is_never_adopted(self):
        # Early refusal precedes any file/private-context access. A caller must
        # select the exact original D28 allocation; None retains primary only.
        with mock.patch.object(driver, "file_hash", side_effect=AssertionError("file read forbidden")):
            for value in ("0" * 64, "", True):
                with self.assertRaisesRegex(ValueError, "unreviewed gateway allocation"):
                    driver.actual_context({}, allocation_sha256=value)

    def test_process_stat_uses_reserved_original_pid_start_and_owned_group(self):
        tail = ["S", "30", "40", "40", *(["0"] * 15), "123456"]
        raw = "40 (worker name (extra)) " + " ".join(tail)
        actual = contract.process_identity(raw, 40, "/opt/recall/bin/elephant", "b" * 64)
        self.assertEqual((actual["pid"], actual["ppid"], actual["start_ticks"]), (40, 30, 123456))
        for changed in (raw.replace("40 (", "41 (", 1), raw.replace(") S ", ") Z "), raw.replace("S 30 40 40", "S 30 99 40")):
            with self.assertRaises(ValueError):
                contract.process_identity(changed, 40, "/opt/recall/bin/elephant", "b" * 64)

    def test_typed_denial_rejects_transport_failure_success_or_extra_payload(self):
        response = {"status": 403, "framing_complete": True, "error_type": None,
                    "body": {"error_code": "forbidden_space", "message": "Denied", "retryable": False}}
        contract.denied(response, 403, "forbidden_space")
        for update in ({"status": 200}, {"error_type": "TimeoutError"}, {"framing_complete": False},
                       {"body": {**response["body"], "results": ["private"]}}):
            with self.assertRaises(ValueError):
                contract.denied({**response, **update}, 403, "forbidden_space")

    def test_strict_json_refuses_duplicate_nonfinite_and_overflow_values(self):
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":1e999}'):
            with self.assertRaises(ValueError):
                contract.strict_json(raw)

    def test_regular_private_reader_rejects_alias_overflow_and_fifo_without_blocking(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name).resolve()
            path = root / "original"
            path.write_bytes(b"original")
            path.chmod(0o600)
            self.assertEqual(files.read_bound(path, private=True), b"original")
            alias = root / "alias"
            alias.symlink_to(path)
            fifo = root / "fifo"
            os.mkfifo(fifo)
            for other in (alias, fifo):
                with self.assertRaises(ValueError):
                    files.read_bound(other, private=True)
            with self.assertRaises(ValueError):
                files.read_bound(path, maximum=2)
            path.chmod(0o644)
            with self.assertRaises(ValueError):
                files.read_bound(path, private=True)

    def test_phase_review_binds_actual_static_bytes_without_future_hashes(self):
        value = plan()
        raw_plan = contract.encode(value)
        values = {"/inputs/plan.json": raw_plan}
        names = ("corpus", "expectations", "proposal", "configurations", "conditional_cases", "rendered_map")
        for name in names:
            values["/inputs/" + name + ".json"] = contract.encode({"offline_control": name})
        refs = {name: {"path": "/inputs/" + name + ".json", "sha256": contract.sha(values["/inputs/" + name + ".json"])} for name in names}
        refs["bootstrap_plan"] = {"path": "/inputs/plan.json", "sha256": contract.sha(raw_plan)}
        template = {"format": "recall-primary-provider-run/v1", "execution_authorized": True,
                    "provider_dispatch_authorized": True, "authentication_enabled": True,
                    **{key: value[key] for key in contract.IDENTITIES if key not in ("configuration_sha256", "run_nonce")},
                    "nonce": value["run_nonce"], "expires_at": value["expires_at"],
                    "fixture_timestamp": "2026-10-07T01:00:00Z", "inputs": refs}
        raw_template = contract.encode(template)
        review = {"format": "recall-primary-driver-review/v1", "approved": True,
                  "reviewer": "qualification (independent D lane)", "primary_template_sha256": contract.sha(raw_template),
                  "inputs": {name: reference["sha256"] for name, reference in refs.items()},
                  "dependencies": value["dependencies"], "nonce": value["run_nonce"],
                  "backend": value["backend"], "endpoint": value["endpoint"]}
        values["/inputs/template.json"] = raw_template
        values["/inputs/review.json"] = contract.encode(review)
        with mock.patch.object(driver, "static_input", side_effect=lambda path: values[str(path)]):
            phase = driver.phase_inputs(Path("/inputs/plan.json"), Path("/inputs/review.json"), Path("/inputs/template.json"))
            self.assertEqual(phase["plan_raw"], raw_plan)
            self.assertNotIn("review", phase["plan"])
            self.assertNotIn("configuration_sha256", phase["template"])
            original_map = values["/inputs/rendered_map.json"]
            values["/inputs/rendered_map.json"] = b"foreign rendered map"
            with self.assertRaisesRegex(ValueError, "immutable reviewed input"):
                driver.phase_inputs(Path("/inputs/plan.json"), Path("/inputs/review.json"), Path("/inputs/template.json"))
            values["/inputs/rendered_map.json"] = original_map
            without_map = copy.deepcopy(template)
            del without_map["inputs"]["rendered_map"]
            changed_template = contract.encode(without_map)
            changed_review = {**review, "primary_template_sha256": contract.sha(changed_template)}
            values["/inputs/template.json"] = changed_template
            values["/inputs/review.json"] = contract.encode(changed_review)
            with self.assertRaisesRegex(ValueError, "contract fields"):
                driver.phase_inputs(Path("/inputs/plan.json"), Path("/inputs/review.json"), Path("/inputs/template.json"))
            values["/inputs/template.json"] = raw_template
            values["/inputs/review.json"] = contract.encode(review)
            values["/inputs/corpus.json"] = b"changed after review"
            with self.assertRaisesRegex(ValueError, "immutable reviewed input"):
                driver.phase_inputs(Path("/inputs/plan.json"), Path("/inputs/review.json"), Path("/inputs/template.json"))

    def test_original_template_cannot_smuggle_a_future_dynamic_field(self):
        value = plan()
        raw_plan = contract.encode(value)
        template = {"configuration_sha256": "f" * 64}
        raw_template = contract.encode(template)
        review = {"format": "recall-primary-driver-review/v1", "approved": True,
                  "reviewer": "qualification (independent D lane)", "primary_template_sha256": contract.sha(raw_template),
                  "inputs": {"bootstrap_plan": contract.sha(raw_plan)}, "dependencies": value["dependencies"]}
        with mock.patch.object(driver, "static_input", side_effect=[raw_plan, contract.encode(review), raw_template]), \
             self.assertRaisesRegex(ValueError, "contract fields"):
            driver.phase_inputs(Path("/inputs/plan"), Path("/inputs/review"), Path("/inputs/template"))

    def test_dynamic_manifest_join_preserves_template_and_never_adds_permission(self):
        value = plan()
        template = {"execution_authorized": False, "provider_dispatch_authorized": False,
                    "inputs": {"bootstrap_plan": {"path": "/inputs/original", "sha256": "a" * 64}}}
        phase = {"plan": value, "template": template,
                 "review_reference": {"path": "/inputs/review", "sha256": "b" * 64}}
        result = driver.primary_manifest(phase, receipt(value), {"path": "/work/receipt", "sha256": "c" * 64},
                                         {"path": "/work/context", "sha256": "d" * 64})
        self.assertFalse(result["execution_authorized"])
        self.assertFalse(result["provider_dispatch_authorized"])
        self.assertNotIn("review", template["inputs"])
        self.assertNotIn("configuration_sha256", template)
        self.assertEqual(result["inputs"]["bootstrap_plan"], template["inputs"]["bootstrap_plan"])
        self.assertEqual(result["token_files"], receipt(value)["token_files"])

    def test_actual_gateway_context_required_before_phase_can_construct_service(self):
        phase = {"plan": plan(), "review": {}}
        with mock.patch.object(driver, "ROOT", Path("/host/checkout")), \
             mock.patch.object(driver, "file_hash", side_effect=AssertionError("artifact read forbidden")), \
             self.assertRaisesRegex(ValueError, "actual packaged"):
            driver.actual_context(phase)

    def test_bounded_http_capture_retains_original_raw_response_without_bearer(self):
        saved, sent = {}, []
        class Writer:
            def write(self, value): sent.append(value)
            async def drain(self): pass
            def close(self): pass
            async def wait_closed(self): pass
        async def frame(reader, observed):
            raw = b'{"error_code":"forbidden_space","message":"Denied","retryable":false}'
            observed.update(raw_body=raw, framing_complete=True, status=403, body=contract.strict_json(raw))
        wire = SimpleNamespace(HEADER_CAP=32768, read_frame=frame)
        allocation = SimpleNamespace(remaining=lambda: 10)
        capture = files.PublicCapture(wire, Path("/work/owned"), allocation)
        capture.tokens["service-household"] = "synthetic.token.value"
        async def exercise():
            with mock.patch.object(files.asyncio, "open_connection", new=mock.AsyncMock(return_value=(object(), Writer()))), \
                 mock.patch.object(files, "private_new", side_effect=lambda path, raw: saved.__setitem__(str(path), raw)):
                return await capture.request("GET", "/v1/spaces/household/sources?limit=100", role="service-household", space="household")
        result = asyncio.run(exercise())
        contract.denied(result, 403, "forbidden_space")
        self.assertEqual(len(sent), 1)
        self.assertIn(b"Authorization: Bearer synthetic.token.value", sent[0])
        self.assertTrue(all(b"synthetic.token.value" not in raw for raw in saved.values()))
        self.assertEqual(len(capture.captures), 1)
        record = capture.captures[0]
        self.assertEqual(contract.sha(saved[record["request_path"]]), record["request_sha256"])
        response = contract.strict_json(saved[record["response_path"]])
        self.assertEqual(contract.sha(base64.b64decode(response["raw_body_base64"])), response["body_sha256"])

    def test_owned_cleanup_escalates_only_the_same_retained_child(self):
        calls = []
        child = SimpleNamespace(reaped=False, threads=[])
        def wait(timeout):
            calls.append(("wait", timeout))
            if len([entry for entry in calls if entry[0] == "wait"]) == 1:
                raise subprocess.TimeoutExpired("owned", timeout)
            child.reaped = True
            return -9
        child.wait = wait
        child.signal = lambda kind: calls.append(("signal", kind))
        processes = files.Processes(None, Path("/work/owned"), SimpleNamespace(remaining=lambda **_: 20))
        processes.items = [child]
        processes.records = [{"index": 1, "logs": [], "exit_code": None}]
        failures = asyncio.run(processes.cleanup())
        self.assertIn("IncompleteGracefulShutdown", {row["error_type"] for row in failures})
        self.assertIn("NonzeroExit", {row["error_type"] for row in failures})
        self.assertEqual([value for kind, value in calls if kind == "signal"], [signal.SIGTERM, signal.SIGKILL])
        self.assertTrue(child.reaped)
        self.assertEqual(processes.records[0]["exit_code"], -9)

    def test_failed_constructor_retains_actual_child_and_original_cleanup_deadline(self):
        child = SimpleNamespace(pid=123, reaped=True, threads=[])
        error = RuntimeError("synthetic post-spawn initialization failure")
        error.owned_process, error.owned_cleanup_errors = child, ["synthetic cleanup failure"]
        owned = mock.Mock(side_effect=error)
        allocation = SimpleNamespace(remaining=lambda **_: 2)
        processes = files.Processes(SimpleNamespace(OwnedProcess=owned), Path("/work/owned"), allocation)
        with mock.patch.object(files.time, "monotonic", return_value=100), \
             self.assertRaisesRegex(RuntimeError, "post-spawn"):
            processes.launch(Path("/opt/recall/bin/elephant"), ["serve"], {"PRIVATE_TOKEN": "not-persisted"},
                             purpose="owned-service", cwd=Path("/work/owned/service"))
        self.assertEqual(owned.call_args.kwargs["initialization_deadline"], 102)
        self.assertIs(processes.items[0], child)
        self.assertEqual(processes.records[0]["pid"], child.pid)
        self.assertTrue(processes.records[0]["initialization_cleanup_failed"])
        self.assertNotIn("not-persisted", contract.encode(processes.records).decode())
        with mock.patch.object(files, "file_hash", return_value="a" * 64):
            failures = asyncio.run(processes.cleanup())
        self.assertTrue(any(row["stage"] == "owned-process-initialization" for row in failures))

    def test_actual_shared_prelaunch_proof_is_passed_to_the_same_owned_constructor(self):
        proof, journal = object(), object()
        capture = SimpleNamespace(CaptureJournal=mock.Mock(return_value=journal),
                                  ProcessProof=mock.Mock(return_value=proof),
                                  deadline_seconds=mock.Mock(return_value=100.25))
        child = SimpleNamespace(pid=123)
        owned = mock.Mock(return_value=child)
        reserve = mock.Mock()
        allocation = SimpleNamespace(remaining=lambda **_: 20, deadline_ns=100_250_000_000)
        processes = files.Processes(SimpleNamespace(OwnedProcess=owned), Path("/work/owned"), allocation,
                                    capture_module=capture, reserve_capture=reserve)
        with mock.patch.object(files.time, "monotonic", return_value=100):
            result = processes.launch(Path("/opt/recall/bin/elephant"), ["serve"], {"SECRET": "unrecorded-value"},
                                      purpose="owned-service", cwd=Path("/work/owned/service"))
        self.assertIs(result, child)
        self.assertIs(owned.call_args.kwargs["capture_proof"], proof)
        self.assertEqual(capture.ProcessProof.call_args.args[-1], allocation.deadline_ns)
        self.assertEqual(capture.ProcessProof.call_args.kwargs, {"journal": journal, "index": 1})
        self.assertEqual(owned.call_args.kwargs["initialization_deadline"], 100.25)
        capture.deadline_seconds.assert_called_once_with(allocation.deadline_ns)
        capture.CaptureJournal.assert_called_once_with(Path("/work/owned"), reserve=reserve)
        self.assertNotIn("unrecorded-value", contract.encode(processes.records).decode())

    def test_process_metadata_and_http_share_one_original_capture_budget(self):
        capture = files.PublicCapture(None, Path("/work/owned"), None)
        capture.reserve_capture(123)
        capture.reserve_capture(contract.LIMITS["capture_bytes"] - 123)
        for invalid in (1, True, -1):
            with self.assertRaises(ValueError):
                capture.reserve_capture(invalid)
            self.assertEqual(capture.total_bytes, contract.LIMITS["capture_bytes"])

    def test_pre_spawn_constructor_failure_never_invents_an_owned_handle(self):
        processes = files.Processes(SimpleNamespace(OwnedProcess=mock.Mock(side_effect=OSError("no spawn"))),
                                    Path("/work/owned"), SimpleNamespace(remaining=lambda **_: 20))
        with self.assertRaises(OSError):
            processes.launch(Path("/opt/recall/bin/elephant"), ["serve"], {},
                             purpose="owned-service", cwd=Path("/work/owned/service"))
        self.assertEqual(processes.items, [])
        self.assertEqual(processes.records, [])

    def test_cleanup_signal_failure_preserves_group_and_reaps_other_owned_children(self):
        signals, waits = [], []
        children = []
        for number in (1, 2):
            child = SimpleNamespace(reaped=False, threads=[])
            def signal_child(kind, number=number):
                signals.append((number, kind))
                if number == 2:
                    raise PermissionError("synthetic signal refusal")
            def wait_child(timeout, number=number, child=child):
                waits.append((number, timeout))
                child.reaped = True
                return 0
            child.signal, child.wait = signal_child, wait_child
            children.append(child)
        processes = files.Processes(None, Path("/work/owned"), SimpleNamespace(remaining=lambda **_: 20))
        processes.items = children
        processes.records = [{"index": number, "logs": [], "exit_code": None} for number in (1, 2)]
        failures = asyncio.run(processes.cleanup())
        self.assertEqual([number for number, _ in waits], [1])
        self.assertTrue(children[0].reaped)
        self.assertFalse(children[1].reaped)
        self.assertIsNone(processes.records[1]["log_sha256"])
        self.assertIn((1, signal.SIGTERM), signals)
        self.assertTrue(any(row["error_type"] == "PermissionError" for row in failures))
        self.assertTrue(any(row["error_type"] == "UnreapedOwnedChild" for row in failures))

    def test_cleanup_uses_no_new_wait_allowance_after_original_deadline(self):
        waits = []
        child = SimpleNamespace(reaped=False, threads=[], signal=lambda _: None)
        def wait(timeout):
            waits.append(timeout)
            if len(waits) == 1:
                raise subprocess.TimeoutExpired("owned", timeout)
            child.reaped = True
            return -9
        child.wait = wait
        allocation = SimpleNamespace(remaining=mock.Mock(side_effect=ValueError("expired")))
        processes = files.Processes(None, Path("/work/owned"), allocation)
        processes.items = [child]
        processes.records = [{"index": 1, "logs": [], "exit_code": None}]
        failures = asyncio.run(processes.cleanup())
        self.assertEqual(waits, [0, 0])
        self.assertTrue(any(row["error_type"] == "DeadlineExpired" for row in failures))
        self.assertIsNone(processes.records[0]["log_sha256"])

    def test_cleanup_nonzero_exit_and_unfinished_logs_cannot_report_success(self):
        for code, alive in ((1, False), (0, True), (0, False)):
            with self.subTest(code=code, alive=alive):
                child = SimpleNamespace(reaped=False, threads=[SimpleNamespace(is_alive=lambda: alive)], signal=lambda _: None)
                def wait(_):
                    child.reaped = True
                    return code
                child.wait = wait
                processes = files.Processes(None, Path("/work/owned"), SimpleNamespace(remaining=lambda **_: 20))
                processes.items = [child]
                processes.records = [{"index": 1, "logs": [], "exit_code": None}]
                failures = asyncio.run(processes.cleanup())
                self.assertEqual(bool(failures), code != 0 or alive)
                self.assertEqual(processes.records[0]["log_sha256"], None if alive else {})

    def test_failed_first_backend_suppresses_second_phase(self):
        first, second = plan(), plan()
        second["backend"] = "surreal-db"
        phases = [{"plan": first}, {"plan": second}]
        with mock.patch.object(driver, "actual_context", return_value=None), \
             mock.patch.object(driver, "execute_phase", new=mock.AsyncMock(side_effect=ValueError("owned service cleanup was incomplete"))) as phase, \
             self.assertRaisesRegex(ValueError, "cleanup was incomplete"):
            asyncio.run(driver.execute(phases))
        self.assertEqual(phase.await_count, 1)
        self.assertIs(phase.await_args.args[0], phases[0])


if __name__ == "__main__":
    unittest.main()
