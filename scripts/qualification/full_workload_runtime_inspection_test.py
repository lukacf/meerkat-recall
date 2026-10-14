"""Closed-world recipe controls; no actual process, HTTP, service or provider."""
from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import unittest
from unittest import mock

import full_workload_runtime_inspection as inspection


def expectations():
    factory = {"max_total_http_attempts": 3, "max_inputs_per_call": 100,
               "initial_segment_bytes": 2048, "max_split_depth": 16}
    return {"format": "recall-full-workload-worker-expectations/v1",
            "work_queue": {"enabled": True, "max_concurrent": 1, "max_extract_concurrent": 1,
                "work_item_timeout_secs": 420, "transient_retry_max_attempts": 3,
                "transient_retry_base_delay_secs": 10, "transient_retry_max_delay_secs": 120,
                "allowed_kinds": [], "disabled_kinds": [], "doc_tree": {
                    "max_chunk_size": 512, "chunk_overlap": 50, "create_paragraphs": True,
                    "detect_sections": False, "semantic_chunking": True, "create_summaries": False,
                    "summary_threshold": 1000, "summarizer_model": "claude-sonnet-4-5"}},
            "embed_worker": {"enabled": True, "batch_size": 100, "provider": "unconfigured",
                             "model": "text-embedding-3-large", "dimensions": 3072},
            "embedding_execution": {name: {key: copy.deepcopy(factory) for key in
                ("query_embedding", "document_embedding")} for name in inspection.SPACES}}


def plan(backend="elephant-db"):
    configurations = {}
    for name, dimensions in zip(inspection.SPACES, (1536, 3072)):
        model = "text-embedding-3-small" if dimensions == 1536 else "text-embedding-3-large"
        configurations[name] = {"configuration": {"space_id": name,
            "security": {"policy_id": name, "level": 3 if dimensions == 1536 else 7},
            "processing": {"generation": 1, "index_generation": 0, "policy_id": name,
                "active_embedding_profile_id": "embedding-full-v2", "pending_reindex": None,
                "profiles": {"embedding-full-v2": {"id": "embedding-full-v2", "connection_id": "owned",
                    "model": model, "model_version": "controlled-fixture-v1", "dimensions": dimensions,
                    "preprocessing": "text-full-v2"}},
                "connections": {"owned": {"id": "owned", "provider": "openai", "account_id": name,
                    "endpoint": "http://127.0.0.1:43132/v1", "credential_ref": "FULL_WORKLOAD_SPY_KEY",
                    "credential_generation": 1, "revoked": False}},
                "bindings": {key: {"id": key + "-full-v2", "profile_id": "embedding-full-v2", "revoked": False}
                             for key in ("query_embedding", "document_embedding")}}}}
    return {"backend": backend, "run_nonce": "original-full-allocation", "configurations": configurations,
            "worker_runtime_expectations": expectations()}


class Capture:
    def __init__(self, root):
        self.directory = root
        self.written = self.held = 0
        self.maximum = 1048576

    def reserve(self, size):
        inspection.require(self.written + self.held + size <= self.maximum, "original capture budget exhausted")
        self.held += size


class OriginalClient:
    """Synthetic original raw captures, never a canned acceptance receipt."""
    def __init__(self, root, original):
        self.capture = Capture(root)
        self.count, self.deadline, self.expires, self.now = 0, 100, 10**12, 1
        self.clock = lambda: self.now
        self.tokens = {"runtime:inspect": "synthetic-original-token"}
        expected = original["worker_runtime_expectations"]
        self.worker = {"format": "recall-managed-worker-recipe/v1", "backend": original["backend"],
            "instance_id": "11111111-1111-4111-8111-111111111111",
            "work_queue": copy.deepcopy(expected["work_queue"]), "embed_worker": copy.deepcopy(expected["embed_worker"])}
        self.configs, self.statuses, self.rows, self.calls = {}, {}, [], []
        self.alter = lambda row: None
        for number, name in enumerate(inspection.SPACES, 1):
            config = copy.deepcopy(original["configurations"][name]["configuration"])
            config["id"] = "space_config:" + str(number) * 36
            config["updated_at"] = "2026-10-07T00:00:00Z"
            self.configs[name] = config
            processing = config["processing"]
            recipes = {}
            for operation, binding in processing["bindings"].items():
                profile = processing["profiles"][binding["profile_id"]]
                connection = processing["connections"][profile["connection_id"]]
                recipes[operation] = {**copy.deepcopy(expected["embedding_execution"][name][operation]),
                    "provenance": {"config": {"space_id": name, "config_id": config["id"],
                        "generation": 1, "policy_id": name}, "operation": operation,
                        "binding_id": binding["id"], "profile_id": profile["id"]},
                    "partition": {"space_id": name, "provider": connection["provider"], "account_id": name,
                        "endpoint": connection["endpoint"], "profile_id": profile["id"], "model": profile["model"],
                        "model_version": profile["model_version"], "dimensions": profile["dimensions"],
                        "preprocessing": profile["preprocessing"]}}
            self.statuses[name] = {"space": name, "processing_generation": 1, "embedding_execution_recipes": recipes}

    def save(self, method, path, space, role, body, request):
        self.count += 1
        row = {"id": self.count, "method": method, "path": path, "space": space,
               "credential_role": role, "status": 200, "framing_complete": True, "body": copy.deepcopy(body)}
        self.alter(row)
        for suffix, value in (("request.json", request), ("response.bin", row["body"]), ("exchange.json", row)):
            raw = inspection.encode(value)
            self.capture.reserve(len(raw))
            (self.capture.directory / f"{self.count:06}.{suffix}").write_bytes(raw)
            self.capture.written += len(raw)
            self.capture.held -= len(raw)
        self.rows.append(row)
        self.calls.append((method, path, space, role, request))
        return row

    async def runtime_request(self, body, response_limit=None):
        assert response_limit == 262144
        if body["method"] == "initialize":
            result = {"protocolVersion": "2025-03-26"}
        else:
            result = {"content": [{"type": "text", "text": json.dumps({"managed_worker_recipe": self.worker})}],
                      "isError": False}
        return self.save("POST", "/mcp/full", None, "runtime:inspect",
                         {"jsonrpc": "2.0", "id": body["id"], "result": result}, body)

    async def request(self, method, path, name, *, role, response_limit):
        assert response_limit == 262144 and role == "manage" and method == "GET"
        body = self.statuses[name] if path.endswith("/status") else self.configs[name]
        return self.save(method, path, name, name + ":manage", body, None)


class OriginalOwner:
    def __init__(self, client, original):
        self.client, self.calls = client, 0
        process = {"executable_sha256": "a" * 64,
                   "process": {"pid": 100, "ppid": 99, "pgrp": 100, "session": 100, "start_ticks": 555}}
        paired = copy.deepcopy(process) if original["backend"] == "surreal-db" else None
        if paired is not None:
            paired["executable_sha256"] = "b" * 64
            paired["process"].update(pid=200, pgrp=200, session=200)
        self.identity = {"format": "recall-full-workload-process-identity/v1", "backend": original["backend"],
                         "run_nonce": original["run_nonce"], "processes": {"recall": process, "paired": paired}}
        self.alter = lambda owner: None

    async def revalidate_original(self):
        self.calls += 1
        self.alter(self)
        return copy.deepcopy(self.identity)


class InspectionControls(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        for target in ("subprocess.Popen", "socket.create_connection", "asyncio.open_connection"):
            patcher = mock.patch(target, side_effect=AssertionError("actual process or network forbidden"))
            patcher.start()
            self.addCleanup(patcher.stop)
        self.serial = 0

    def fixture(self, backend="elephant-db", original=None):
        self.serial += 1
        root = Path(self.temp.name) / str(self.serial)
        root.mkdir()
        original = original or plan(backend)
        client = OriginalClient(root, original)
        owner = OriginalOwner(client, original)
        return inspection.RuntimeRecipeInspector(client, original, owner), client, owner

    def test_all_four_boundaries_retain_original_raws_and_identity_for_both_backends(self):
        for backend in ("elephant-db", "surreal-db"):
            with self.subTest(backend=backend):
                inspector, client, owner = self.fixture(backend)
                for boundary in inspection.BOUNDARIES:
                    self.assertIsNone(asyncio.run(inspector.capture(boundary)))
                self.assertEqual(client.count, 21)
                self.assertEqual(owner.calls, 8)
                self.assertEqual(inspector.completed, list(inspection.BOUNDARIES))
                self.assertEqual(inspector.original["worker"]["request_index"], 2)
                self.assertEqual(client.capture.held, 0)
                self.assertEqual(client.capture.written, sum(p.stat().st_size for p in client.capture.directory.iterdir()))
                self.assertEqual(len(list(client.capture.directory.glob("*.response.bin"))), 21)
                self.assertEqual(len(list(client.capture.directory.glob("runtime-inspection-*.json"))), 4)
                self.assertEqual([call[2:4] for call in client.calls if call[1] == "/mcp/full"],
                                 [(None, "runtime:inspect")] * 5)

    def test_missing_effective_expectation_and_bool_integer_never_infer_defaults(self):
        for section, field in (("work_queue", "doc_tree"), ("embed_worker", "model"),
                               ("embedding_execution", inspection.SPACES[1])):
            original = plan()
            del original["worker_runtime_expectations"][section][field]
            with self.subTest(section=section), self.assertRaises(ValueError):
                inspection.validate_expectations(original["worker_runtime_expectations"])
        for field in ("max_concurrent", "transient_retry_max_attempts"):
            expected = expectations()
            expected["work_queue"][field] = True
            with self.subTest(field=field), self.assertRaises(ValueError):
                inspection.validate_expectations(expected)

    def test_actual_worker_values_must_match_complete_reviewed_recipe(self):
        changes = (("work_queue", "max_concurrent", 2), ("work_queue", "disabled_kinds", ["render_doc"]),
                   ("embed_worker", "enabled", False), ("embed_worker", "dimensions", 1536),
                   ("embed_worker", "provider", "openai"), ("work_queue", "max_concurrent", True))
        for section, field, value in changes:
            inspector, client, _ = self.fixture()
            client.worker[section][field] = value
            with self.subTest(section=section, field=field, value=value), self.assertRaises(ValueError):
                asyncio.run(inspector.capture("initial"))
            self.assertEqual(client.count, 2)
            self.assertIsNone(inspector.original)
        inspector, client, _ = self.fixture()
        client.worker["work_queue"]["doc_tree"]["create_summaries"] = True
        with self.assertRaises(ValueError):
            asyncio.run(inspector.capture("initial"))

    def test_same_values_with_replacement_worker_uuid_are_refused_without_adoption(self):
        inspector, client, _ = self.fixture()
        asyncio.run(inspector.capture("initial"))
        original = copy.deepcopy(inspector.original)
        client.worker["instance_id"] = "22222222-2222-4222-8222-222222222222"
        with self.assertRaisesRegex(ValueError, "construction changed"):
            asyncio.run(inspector.capture("before_fw09_first_space"))
        self.assertEqual(inspector.original, original)
        client.worker["instance_id"] = original["worker"]["instance_id"]
        with self.assertRaisesRegex(ValueError, "boundary order"):
            asyncio.run(inspector.capture("before_fw09_first_space"))

    def test_all_factory_limits_and_full_partition_are_checked_for_both_spaces(self):
        for space in inspection.SPACES:
            for group, field, value in ((None, "max_split_depth", 8), (None, "initial_segment_bytes", 1024),
                                       (None, "max_total_http_attempts", 4), (None, "max_inputs_per_call", 128),
                                       ("partition", "dimensions", 8), ("partition", "endpoint", "http://elsewhere/v1"),
                                       ("partition", "preprocessing", "text-v1"),
                                       ("provenance", "binding_id", "replacement")):
                inspector, client, _ = self.fixture()
                recipe = client.statuses[space]["embedding_execution_recipes"]["query_embedding"]
                (recipe[group] if group else recipe)[field] = value
                with self.subTest(space=space, group=group, field=field), self.assertRaises(ValueError):
                    asyncio.run(inspector.capture("initial"))
                self.assertIsNone(inspector.original)

    def test_configuration_identity_change_after_initial_is_not_adopted(self):
        inspector, client, _ = self.fixture()
        asyncio.run(inspector.capture("initial"))
        name = inspection.SPACES[1]
        client.configs[name]["id"] = "space_config:" + "3" * 36
        for recipe in client.statuses[name]["embedding_execution_recipes"].values():
            recipe["provenance"]["config"]["config_id"] = client.configs[name]["id"]
        with self.assertRaisesRegex(ValueError, "configuration or factory changed"):
            asyncio.run(inspector.capture("before_fw09_first_space"))
        self.assertEqual(inspector.completed, ["initial"])

    def test_publication_content_epochs_advance_without_replacing_approved_policy(self):
        inspector, client, _ = self.fixture()
        first_hashes = None
        for ordinal, boundary in enumerate(inspection.BOUNDARIES):
            epochs = {name: ordinal * (number + 7) for number, name in enumerate(inspection.SPACES)}
            for name, epoch in epochs.items():
                client.configs[name]["processing"]["index_generation"] = epoch
            asyncio.run(inspector.capture(boundary))
            raw = (client.capture.directory / ("runtime-inspection-" + boundary + ".json")).read_bytes()
            observation = json.loads(raw)
            hashes = {name: row["sha256"] for name, row in observation["configurations"].items()}
            if first_hashes is None:
                first_hashes = hashes
            self.assertEqual(hashes, first_hashes)
            self.assertEqual(inspector.last_epochs, epochs)
            self.assertEqual({name: row["index_generation"] for name, row in observation["configurations"].items()}, epochs)
            for name in inspection.SPACES:
                row_id = observation["configurations"][name]["request_index"]
                original_body = json.loads((client.capture.directory / f"{row_id:06}.response.bin").read_bytes())
                self.assertEqual(original_body["processing"]["index_generation"], epochs[name])
        self.assertEqual(inspector.original["configurations"][inspection.SPACES[0]]["index_generation"], 0)

    def test_only_content_epoch_can_progress_and_it_cannot_regress_or_change_type(self):
        for change in ("generation", "binding", "boolean", "negative", "overflow", "regression", "initial"):
            inspector, client, _ = self.fixture()
            name = inspection.SPACES[0]
            processing = client.configs[name]["processing"]
            if change == "initial":
                processing["index_generation"] = 1
                boundary = "initial"
            else:
                asyncio.run(inspector.capture("initial"))
                processing["index_generation"] = 7
                asyncio.run(inspector.capture("before_fw09_first_space"))
                boundary = "before_fw09_second_space"
                if change == "generation":
                    processing["generation"] = 2
                elif change == "binding":
                    processing["bindings"]["query_embedding"]["id"] = "replacement"
                else:
                    processing["index_generation"] = {"boolean": True, "negative": -1,
                        "overflow": 2**64, "regression": 6}[change]
            with self.subTest(change=change), self.assertRaises(ValueError):
                asyncio.run(inspector.capture(boundary))

    def test_owner_identity_brackets_requests_and_changed_original_refuses_before_http(self):
        inspector, client, owner = self.fixture()
        asyncio.run(inspector.capture("initial"))
        owner.identity["processes"]["recall"]["process"]["start_ticks"] += 1
        with self.assertRaisesRegex(ValueError, "owned process changed"):
            asyncio.run(inspector.capture("before_fw09_first_space"))
        self.assertEqual(client.count, 6)
        inspector, client, owner = self.fixture()
        def change_after(owner):
            if owner.calls == 2:
                owner.identity["processes"]["recall"]["executable_sha256"] = "f" * 64
        owner.alter = change_after
        with self.assertRaisesRegex(ValueError, "across inspection"):
            asyncio.run(inspector.capture("initial"))
        self.assertIsNone(inspector.original)
        self.assertEqual(len(list(client.capture.directory.glob("*.response.bin"))), 6)

    def test_original_clock_and_capture_exhaustion_never_create_success_index(self):
        inspector, client, owner = self.fixture()
        owner.alter = lambda owner: setattr(owner.client, "now", 100)
        with self.assertRaisesRegex(ValueError, "clock expired"):
            asyncio.run(inspector.capture("initial"))
        self.assertEqual(client.count, 0)
        inspector, client, owner = self.fixture()
        def exhaust_after_reads(owner):
            if owner.calls == 2:
                owner.client.capture.maximum = owner.client.capture.written
        owner.alter = exhaust_after_reads
        with self.assertRaisesRegex(ValueError, "budget exhausted"):
            asyncio.run(inspector.capture("initial"))
        self.assertFalse(list(client.capture.directory.glob("runtime-inspection-*.json")))
        self.assertIsNone(inspector.original)

    def test_incomplete_or_foreign_exchange_cannot_supply_recipe(self):
        for key, value in (("framing_complete", False), ("status", 403), ("space", inspection.SPACES[0]),
                           ("credential_role", "runtime:operator"), ("connection_cleanup_failed", True)):
            inspector, client, _ = self.fixture()
            client.alter = lambda row, key=key, value=value: row.update({key: value})
            with self.subTest(key=key), self.assertRaises(ValueError):
                asyncio.run(inspector.capture("initial"))
            self.assertIsNone(inspector.original)
            self.assertEqual(client.count, 1)

    def test_boundary_order_shared_client_and_explicit_inspector_credential_are_required(self):
        inspector, client, owner = self.fixture()
        with self.assertRaises(ValueError):
            asyncio.run(inspector.capture("before_fw09_first_space"))
        self.assertEqual(client.count, 0)
        client.tokens.clear()
        with self.assertRaises(ValueError):
            inspection.RuntimeRecipeInspector(client, plan(), owner)
        client.tokens["runtime:inspect"] = "original"
        owner.client = object()
        with self.assertRaises(ValueError):
            inspection.RuntimeRecipeInspector(client, plan(), owner)


if __name__ == "__main__":
    unittest.main()
