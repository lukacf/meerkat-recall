"""Offline orchestration controls only; no socket, service, SDK, or provider call."""
from __future__ import annotations

import asyncio
import ast
import base64
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch
import urllib.parse

import primary_provider_driver as driver
import primary_provider_driver_contract as contract
import primary_provider_driver_startup as startup
from primary_provider_driver_transport import PublicTransport, sse_rpc

ROOT = Path(os.environ.get("RECALL_PRIMARY_SOURCE_ROOT", str(driver.ROOT)))


def rendered_map():
    path = Path(os.environ.get("RECALL_PRIMARY_RENDERED_MAP_PATH", str(ROOT / "tests/recall_preview/primary-rendered-map.json")))
    raw = path.read_bytes()
    contract.require(contract.sha(raw) == contract.RENDERED_MAP_SHA256, "actual independent map bytes changed")
    return json.loads(raw)


def fixtures():
    corpus = json.loads((ROOT / "tests/recall_preview_semantics/corpus.json").read_text())
    proposal = json.loads((ROOT / "tests/recall_preview/primary-provider-proposal.json").read_text())
    expectations = json.loads((ROOT / "tests/recall_preview/semantic-expectations-approval.json").read_text())
    return corpus, expectations, proposal


class ContractControls(unittest.TestCase):
    def test_isolated_inert_invocation_uses_only_bundled_siblings(self):
        with tempfile.TemporaryDirectory() as directory:
            hostile = Path(directory) / "primary_provider_driver_contract.py"
            hostile.write_text("raise RuntimeError('ambient sibling was imported')\n")
            result = subprocess.run(
                [sys.executable, "-I", "-B", str(Path(driver.__file__).resolve())],
                cwd=directory, env={"PYTHONPATH": directory}, capture_output=True,
                timeout=10, check=True,
            )
        observed = json.loads(result.stdout)
        self.assertEqual(observed["format"], "recall-primary-driver-inventory/v1")
        self.assertFalse(observed["execution_authorized"])
        self.assertFalse(observed["qualified"])
        self.assertEqual(result.stderr, b"")

    def test_exact_frozen_cases_and_no_relevance_filter(self):
        corpus, expectations, proposal = fixtures()
        contract.corpus_contract(corpus, expectations, proposal)
        for query in corpus["queries"]:
            for mode in contract.MODES:
                request = contract.search_request(corpus, query, mode)
                self.assertEqual(set(request), {"query", "limit", "ranking_mode", "source_revision_constraints"})
                self.assertEqual(request["query"], query["query"])
                self.assertEqual(request["limit"], 3)
                self.assertEqual(request["source_revision_constraints"], contract.current_inventory(corpus, query["space"]))
                self.assertGreater(len(request["source_revision_constraints"]), 3)
        altered = copy.deepcopy(corpus)
        altered["queries"][0]["query"] += " rescue"
        with self.assertRaises(ValueError):
            contract.corpus_contract(altered, expectations, proposal)

    def test_request_keeps_original_bytes_and_explicit_replacement_generation(self):
        corpus, _, _ = fixtures()
        source = corpus["sources"][0]
        security = {"policy_id": "synthetic-household", "level": 3, "labels": [], "handling": []}
        first = contract.source_request(source, "elephant-db", "originalnonce123456", "2026-10-07T00:00:00Z", security, None)
        second_source = {**source, "revision": "next", "text": "é changed original input"}
        second = contract.source_request(second_source, "elephant-db", "originalnonce123456", "2026-10-07T00:00:00Z", security, "actual-original-generation")
        self.assertEqual(first["source"]["id"], second["source"]["id"])
        self.assertNotEqual(first["doc"]["id"], second["doc"]["id"])
        self.assertEqual(second["intake"]["expected_generation"], "actual-original-generation")
        self.assertEqual(second["source"]["hash"], contract.sha(second_source["text"].encode()))
        self.assertEqual(first["ingest_mode"], "graph")
        self.assertEqual(first["doc"]["security"], security)

    def test_original_operation_substitution_and_pending_never_complete(self):
        original = {"operation_id": "original", "generation": "g1", "internal_source_id": "source:original", "desired_revision": "r1"}
        complete = {**original, "state": "complete", "lifecycle": "active", "published_revision": "r1", "error_code": None}
        contract.original_operation(original, complete, True)
        for patch in ({"operation_id": "later"}, {"generation": "g2"}, {"published_revision": "r0"}, {"state": "pending"}):
            with self.assertRaises(ValueError):
                contract.original_operation(original, {**complete, **patch}, True)

    def test_mcp_original_id_and_complete_stream_required(self):
        raw = b'event: message\ndata: {"jsonrpc":"2.0","id":7,"result":{"ok":true}}\n\n'
        self.assertEqual(sse_rpc(raw, json.loads, 7)["id"], 7)
        for bad in (raw[:-1], raw + raw, raw.replace(b'"id":7', b'"id":8')):
            with self.assertRaises(ValueError):
                sse_rpc(bad, json.loads, 7)

    def test_failed_framing_never_qualifies_success(self):
        for response in ({"status": 200, "framing_complete": False, "body": {}},
                         {"status": 200, "framing_complete": True, "body": {}, "error_type": "TimeoutError"}):
            with self.assertRaises(ValueError):
                PublicTransport.successful(response)

    def test_conditional_omission_requires_reviewed_reason(self):
        cases = {"format": "recall-primary-conditional-cases/v1", "reviewer": "qualification (independent D lane)",
                 "status": "approved-before-execution", "cases": [],
                 "disposition": {name: {"status": "not-applicable", "reason": "offline control only"}
                                 for name in ("wiki", "truth_maintenance")}}
        self.assertEqual(contract.conditional_contract(cases, {"space"}), [])
        cases["disposition"]["wiki"] = {"status": "required", "reason": "required before execution"}
        with self.assertRaises(ValueError):
            contract.conditional_contract(cases, {"space"})

    def test_independent_rendered_map_binds_all_original_input_bytes(self):
        corpus, _, _ = fixtures()
        expected = rendered_map()
        raw_sha = contract.sha((ROOT / "tests/recall_preview_semantics/corpus.json").read_bytes())
        rows = contract.rendered_contract(expected, corpus, raw_sha)
        self.assertEqual(len(rows), 28)
        for original in corpus["sources"]:
            row = rows[(original["space"], original["id"], original["revision"])]
            self.assertEqual(row["rendered_text"], original["text"] + "\n")
            self.assertEqual(row["paragraph_spans"][0]["end"], len(original["text"].encode()))
        for change in ("missing", "foreign", "text", "span", "corpus"):
            altered = copy.deepcopy(expected)
            if change == "missing":
                altered["sources"].pop()
            elif change == "foreign":
                altered["sources"][0]["space"] = "different-space"
            elif change == "text":
                altered["sources"][0]["rendered_text"] = altered["sources"][0]["rendered_text"].rstrip()
            elif change == "span":
                altered["sources"][0]["chunk_spans"][0]["end"] = 10**9
            else:
                altered["corpus_sha256"] = "0" * 64
            with self.subTest(change=change), self.assertRaises(ValueError):
                contract.rendered_contract(altered, corpus, raw_sha)

    def test_actual_rendered_evidence_requires_exact_lf_and_complete_spans(self):
        source = fixtures()[0]["sources"][0]
        row = next(row for row in rendered_map()["sources"] if (row["space"], row["source_key"], row["revision"]) ==
                   (source["space"], source["id"], source["revision"]))
        accepted = {"doc_id": "doc:original", "internal_source_id": "source:original"}
        base = {"space_id": source["space"], "security": {}, "deleted_at": None}
        doc = {**base, "id": accepted["doc_id"], "source": accepted["internal_source_id"], "text": row["rendered_text"]}
        record = {**base, "id": accepted["internal_source_id"], "external_id": source["id"], "hash": contract.sha(source["text"].encode()),
                  "current_ingest_mode": "graph", "lifecycle_status": "graph_ready"}
        nodes = [{**base, "id": "doc_node:" + role, "role": role, "revision": "doc_revision:original",
                  "span": row["root_span"] if role == "document" else row[role + "_spans"][0],
                  "embedding_model": "original-model"} for role in ("document", "paragraph", "chunk")]
        pipeline = {"doc_id": accepted["doc_id"], "revision": "doc_revision:original", "nodes_count": 3,
                    "chunks": {"total": 2, "embedded": 2}}
        expected = {"source_security": {}, "embedding_partition_model": "original-model"}
        contract.publication_evidence(source, accepted, doc, record, pipeline, nodes, expected, row)
        # Use the actual NodeRole wire value. The former synthetic "root"
        # fixture must refuse, as must aliases or unrelated structural roles.
        for role in ("root", "Document", "document ", "section", "summary", "unknown"):
            wrong = copy.deepcopy(nodes)
            wrong[0]["role"] = role
            with self.subTest(role=role), self.assertRaisesRegex(ValueError, "unreviewed rendered node role"):
                contract.publication_evidence(source, accepted, doc, record, pipeline, wrong, expected, row)
        for change in ("missing-document", "duplicate-document", "short-document-span"):
            wrong = copy.deepcopy(nodes)
            if change == "missing-document":
                wrong.pop(0)
            elif change == "duplicate-document":
                wrong.append({**copy.deepcopy(wrong[0]), "id": "doc_node:another-document"})
            else:
                wrong[0]["span"]["end"] -= 1
            # Match inventory totals so the exact pre-output root-span check,
            # rather than an incidental count mismatch, must reject this.
            matching_count = {**pipeline, "nodes_count": len(wrong)}
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, "rendered coverage differs"):
                contract.publication_evidence(source, accepted, doc, record, matching_count, wrong, expected, row)
        renderer = rendered_map()["renderer"]
        revision = {**base, "id": pipeline["revision"], "doc": accepted["doc_id"], "text": row["rendered_text"],
                    "text_hash": row["rendered_sha256"], "renderer": {"name": renderer["name"], "version": renderer["version"]}, "status": "active"}
        contract.revision_evidence(source, accepted, pipeline["revision"], revision, row, renderer, expected)
        for changes in ({"doc": "doc:another"}, {"space_id": "another-space"}, {"text": source["text"]},
                        {"text_hash": record["hash"]}, {"status": "superseded"}):
            with self.subTest(revision=changes), self.assertRaises(ValueError):
                contract.revision_evidence(source, accepted, pipeline["revision"], {**revision, **changes}, row, renderer, expected)
        with self.assertRaises(ValueError):
            contract.publication_evidence(source, accepted, {**doc, "text": source["text"]}, record, pipeline, nodes, expected, row)
        wrong = copy.deepcopy(nodes)
        wrong[-1]["span"]["end"] += 1
        with self.assertRaises(ValueError):
            contract.publication_evidence(source, accepted, doc, record, pipeline, wrong, expected, row)
        with self.assertRaises(ValueError):
            contract.publication_evidence(source, accepted, doc, {**record, "hash": row["rendered_sha256"]}, pipeline, nodes, expected, row)


class FakePublicTransport:
    """Explicit orchestration fake. Its outputs never represent product evidence."""
    successful = staticmethod(PublicTransport.successful)

    def __init__(self, output, loaded, fail_first=False, fail_query=False):
        self.output, self.loaded = output, loaded
        output.mkdir()
        self.deadline = time.monotonic() + 120
        self.count, self.captures = 0, []
        self.inputs, self.heads, self.operations, self.documents, self.records = [], {}, {}, {}, {}
        self.fail_first, self.fail_query = fail_first, fail_query

    async def request(self, method, path, space, body=None, timeout=15, mcp_id=None):
        self.count += 1
        self.inputs.append((method, path, space, copy.deepcopy(body)))
        response = {"status": 200, "framing_complete": True, "body": {}, "request_index": self.count, "error_type": None}
        if path == "/v1/sources/ingest":
            if self.fail_first:
                self.fail_first = False
                return {**response, "status": 503}
            source, doc = copy.deepcopy(body["source"]), copy.deepcopy(body["doc"])
            key = (space, source["external_id"])
            self_prior = self.heads.get(key)
            if self_prior is not None:
                assert body["intake"]["expected_generation"] == self_prior["generation"]
            operation_id = f"operation-{self.count}"
            head = {"operation_id": operation_id, "internal_source_id": source["id"], "generation": f"generation-{self.count}",
                    "desired_revision": body["intake"]["revision_id"], "published_revision": body["intake"]["revision_id"],
                    "state": "complete", "lifecycle": "active", "error_code": None}
            self.heads[key] = {**head, "external_source_id": source["external_id"]}
            self.operations[operation_id] = head
            doc["source"] = source["id"]
            expected = next(row for row in self.loaded["rendered_map"]["sources"]
                            if (row["space"], row["source_key"], row["revision"]) ==
                               (space, source["external_id"], head["desired_revision"]))
            doc["text"] = expected["rendered_text"]
            self.documents[doc["id"]] = doc
            source.update({"current_ingest_mode": "graph", "lifecycle_status": "graph_ready"})
            self.records[source["id"]] = source
            body = {"source_id": source["id"], "doc_id": doc["id"], "work_item_id": f"work_item:{self.count}",
                    "operation": {"operation_id": operation_id, "source_generation": head["generation"],
                                  "source_id": source["id"], "space_id": space, "desired_revision": head["desired_revision"]}}
            return {**response, "status": 202, "body": body}
        if path.endswith("/search"):
            if self.fail_query:
                self.fail_query = False
                return {**response, "error_type": "TimeoutError", "framing_complete": False, "body": None}
            coverage = {key: {"state": "complete", "requested_revision": revision,
                              "desired_revision": revision, "published_revision": revision}
                        for key, revision in body["source_revision_constraints"].items()}
            return {**response, "body": {"results": [], "coverage": coverage, "partial": False}}
        raise AssertionError("unexpected mutation in fake")

    async def get(self, path, space):
        self.count += 1
        path = urllib.parse.unquote(path)
        if path == "/v1/spaces/" + space:
            return self.loaded["configurations"][space]["configuration"]
        if "/sources?" in path:
            return {"sources": [row for (owner, _), row in self.heads.items() if owner == space], "next_cursor": None}
        if path.endswith("/head"):
            return self.heads[(space, path.split("/sources/", 1)[1][:-5])]
        if path.startswith("/v1/operations/"):
            return self.operations[path.removeprefix("/v1/operations/")]
        if path.startswith("/v1/sources/"):
            return self.records[path.removeprefix("/v1/sources/")]
        if path.startswith("/v1/docs/"):
            rest = path.removeprefix("/v1/docs/")
            doc_id = rest.split("/", 1)[0]
            doc = self.documents[doc_id]
            if "/revisions/" in rest:
                renderer = self.loaded["rendered_map"]["renderer"]
                return {"id": rest.split("/revisions/", 1)[1], "doc": doc_id, "space_id": space,
                        "security": doc["security"], "status": "active", "text": doc["text"],
                        "text_hash": contract.sha(doc["text"].encode()),
                        "renderer": {"name": renderer["name"], "version": renderer["version"], "config_hash": None}}
            if rest.endswith("/pipeline-status"):
                return {"doc_id": doc_id, "revision": "doc_revision:" + doc_id, "chunks": {"total": 2, "embedded": 2}, "nodes_count": 3}
            if rest.endswith("/chunks"):
                rows = []
                for role in ("document", "paragraph", "chunk"):
                    end = len(doc["text"].encode()) - (role != "document")
                    rows.append({"id": "doc_node:" + doc_id + role, "revision": "doc_revision:" + doc_id, "role": role,
                                 "space_id": space, "security": doc["security"], "span": {"start": 0, "end": end},
                                 "text": doc["text"].encode()[:end].decode(), "embedding_model": "recall:offline-control"})
                return rows
            return doc
        raise AssertionError("unexpected read in fake: " + path)


class DriverControls(unittest.TestCase):
    def execute_fake(self, **options):
        corpus, _, _ = fixtures()
        manifest = {"backend": "elephant-db", "nonce": "originalnonce123456", "fixture_timestamp": "2026-10-07T00:00:00Z",
                    "source_commit": "1" * 40, "artifact_sha256": "2" * 64, "configuration_sha256": "3" * 64}
        loaded = {"corpus": corpus, "rendered_map": rendered_map(), "configurations": {}, "conditional_cases": {
            "format": "recall-primary-conditional-cases/v1", "reviewer": "qualification (independent D lane)",
            "status": "approved-before-execution", "cases": [], "disposition": {
                name: {"status": "not-applicable", "reason": "offline orchestration control"} for name in ("wiki", "truth_maintenance")}}}
        for row in corpus["spaces"]:
            security = {"policy_id": row["policy"], "level": row["level"], "labels": [], "handling": []}
            loaded["configurations"][row["id"]] = {"source_security": security,
                "embedding_partition_model": "recall:offline-control", "configuration": {
                    "space_id": row["id"], "security": security, "processing": {"index_generation": 0}}}
        with tempfile.TemporaryDirectory(prefix=".primary-driver-test-", dir=driver.ROOT) as temp:
            transport = FakePublicTransport(Path(temp) / "capture", loaded, **options)
            status = asyncio.run(driver.Primary(manifest, loaded, transport).run())
            observation = json.loads((transport.output / "observation.json").read_text())
            return status, observation, transport.inputs

    def test_original_28_then_48_without_hidden_submissions(self):
        status, observed, requests = self.execute_fake()
        self.assertEqual(status, 0)
        self.assertEqual(len(observed["publications"]), 28)
        self.assertEqual(len(observed["queries"]), 48)
        self.assertEqual(len([r for r in requests if r[1] == "/v1/sources/ingest"]), 28)
        self.assertEqual(observed["harness_retries"], 0)
        self.assertFalse(observed["qualified"])
        self.assertIn("unmet", observed["provider_accounting"])

    def test_failed_acceptance_stays_failed_and_is_never_resubmitted(self):
        status, observed, requests = self.execute_fake(fail_first=True)
        self.assertEqual(status, 1)
        self.assertEqual(len(observed["publications"]), 27)
        self.assertEqual(len(observed["queries"]), 48)
        original = [r for r in requests if r[1] == "/v1/sources/ingest" and r[3]["source"]["external_id"] == "home/water"]
        self.assertEqual(len(original), 1)
        self.assertFalse(observed["capture_complete"])

    def test_failed_query_remains_in_exact_original_sequence(self):
        status, observed, requests = self.execute_fake(fail_query=True)
        self.assertEqual(status, 1)
        self.assertEqual(len(observed["queries"]), 48)
        self.assertEqual(observed["queries"][0]["error_type"], "ValueError")
        self.assertEqual(observed["queries"][0]["capture"]["error_type"], "TimeoutError")
        self.assertEqual(len([r for r in requests if r[1].endswith("/search")]), 48)


class DynamicStartupControls(unittest.TestCase):
    def test_helper_executes_the_same_original_bytes_that_were_reviewed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "original_helper.py"
            source = (b"VALUE = 'original reviewed helper'\n"
                      b"def identity(value: int) -> int: return value\n")
            replacement = b"VALUE = 'later file or cached helper'\n"
            path.write_bytes(source)
            original_read = Path.read_bytes
            def changed_after_read(selected):
                raw = original_read(selected)
                if selected == path:
                    selected.write_bytes(replacement)
                return raw
            with patch.object(Path, "read_bytes", changed_after_read):
                loaded = startup.load_producer(root, path.name, contract.sha(source))
            self.assertEqual(loaded.VALUE, "original reviewed helper")
            self.assertIs(loaded.identity.__annotations__["value"], int,
                          "the caller's future flags cannot alter the reviewed module")
            self.assertEqual(path.read_bytes(), replacement)
            with self.assertRaisesRegex(ValueError, "helper changed"):
                startup.load_producer(root, path.name, contract.sha(source))

    def test_static_review_does_not_require_future_observation_bytes(self):
        self.assertNotIn("runtime_receipt", driver.INPUTS)
        self.assertNotIn("gateway_context", driver.INPUTS)
        self.assertIn("bootstrap_plan", driver.INPUTS)
        self.assertIn("rendered_map", driver.INPUTS)
        self.assertIn("scripts/qualification/runtime_package_bootstrap.py", driver.DEPENDENCIES)
        self.assertIn("scripts/qualification/runtime_package_bootstrap_io.py", driver.DEPENDENCIES)
        source = ast.parse((ROOT / "scripts/qualification/runtime_package_bootstrap_io.py").read_text())
        required = next(ast.literal_eval(node.value) for node in source.body
                        if isinstance(node, ast.Assign)
                        and any(isinstance(target, ast.Name) and target.id == "DEPENDENCIES"
                                for target in node.targets))
        self.assertIn("scripts/qualification/installed_process_capture.py", required)
        self.assertLessEqual(required, set(driver.DEPENDENCIES),
                             "actual bootstrap helpers must fit the exact primary review inventory")
        self.assertFalse(hasattr(startup, "plan_digest"))  # Review binds exact raw P, no projection.
        self.assertFalse(startup.OBSERVATION_CONTRACTS["runtime_receipt"]["qualified"])

    def test_startup_time_is_charged_to_the_original_allocation(self):
        context = {"anchor": {"clock": {"mono_start": 1000., "deadline_mono": 6400.}}}
        receipt = {"allocation_started_monotonic_ns": 1000 * 10**9,
                   "started_monotonic_ns": 1010 * 10**9, "observed_monotonic_ns": 1290 * 10**9,
                   "original_deadline_monotonic_ns": 3700 * 10**9, "whole_deadline_monotonic_ns": 6400 * 10**9}
        deadline = startup.original_deadline(receipt, context, 1300 * 10**9)
        self.assertEqual(deadline, 3700.)
        self.assertEqual(deadline - 1300, 2400)  # No new 2700-second budget.
        for changes in ({"original_deadline_monotonic_ns": 4000 * 10**9},
                        {"whole_deadline_monotonic_ns": 6700 * 10**9},
                        {"allocation_started_monotonic_ns": 999 * 10**9}):
            with self.assertRaises(ValueError):
                startup.original_deadline({**receipt, **changes}, context, 1300 * 10**9)
        with self.assertRaises(ValueError):
            startup.original_deadline(receipt, context, 3700 * 10**9)

    def captured(self):
        body = {"error_code": "forbidden_space", "message": "refused", "retryable": False}
        raw = contract.encode(body)
        request = {"request_index": 1, "harness_attempt": 1, "method": "GET", "port": 43130,
                   "credential_role": "service-a", "space": "a", "path": "/v1/spaces/a/sources?limit=100",
                   "body": None, "request_body_base64": "", "body_sha256": contract.sha(b""), "started_monotonic_ns": 15}
        response = {"request_index": 1, "raw_body_base64": base64.b64encode(raw).decode(),
                    "body_sha256": contract.sha(raw), "finished_monotonic_ns": 20,
                    "status": 403, "framing_complete": True, "error_type": None, "body": body}
        files = {"request": contract.encode(request), "response": contract.encode(response)}
        receipt = {"started_monotonic_ns": 10, "observed_monotonic_ns": 30, "captures": [{"request_index": 1,
                   "request_path": "request", "response_path": "response", "request_sha256": contract.sha(files["request"]),
                   "response_sha256": contract.sha(files["response"])}]}
        helper = SimpleNamespace(LIMITS={"capture_bytes": 10000, "request_bytes": 1000, "response_bytes": 1000},
                                 strict_json=json.loads)
        return receipt, helper, files, request, response, raw

    def test_original_capture_bytes_and_original_request_identity_are_required(self):
        receipt, helper, files, request, response, raw = self.captured()
        observations = startup.captures(receipt, helper, lambda name, _: files[name])
        self.assertEqual(observations[1], (request, response, raw))
        files["response"] += b" "
        with self.assertRaises(ValueError):
            startup.captures(receipt, helper, lambda name, _: files[name])
        files["response"] = contract.encode({**response, "request_index": 2})
        receipt["captures"][0]["response_sha256"] = contract.sha(files["response"])
        with self.assertRaises(ValueError):
            startup.captures(receipt, helper, lambda name, _: files[name])

    def test_rehashed_decoded_body_cannot_replace_the_actual_wire_denial(self):
        receipt, helper, files, request, response, raw = self.captured()
        helper.CHECKS = {"missing_grant"}
        helper.denied = lambda value, status, code: contract.require(
            value["status"] == status and value["body"]["error_code"] == code, "actual denial differs")
        receipt["checks"] = {"missing_grant": [1]}
        startup.check_proofs(receipt, {"spaces": [{"name": "a"}]}, helper, {1: (request, response, raw)})
        for replacement in ({**response, "body": {"error_code": "invented"}},
                            {**response, "framing_complete": False}, {**response, "error_type": "TimeoutError"}):
            with self.assertRaises(ValueError):
                startup.check_proofs(receipt, {"spaces": [{"name": "a"}]}, helper, {1: (request, replacement, raw)})
        with self.assertRaises(ValueError):
            startup.check_proofs(receipt, {"spaces": [{"name": "a"}, {"name": "b"}]}, helper,
                                 {1: (request, response, raw)})

    def command_fixture(self, embedded=False):
        source_root = Path(os.environ.get("RECALL_BOOTSTRAP_SOURCE_ROOT", str(ROOT)))
        helper = startup.load_producer(source_root, startup.C_HELPER,
                                       contract.sha((source_root / startup.C_HELPER).read_bytes()))
        root = "/work/original-backend"
        plan = {"backend": "elephant-db" if embedded else "surreal-db", "run_directory": root}
        service = {"recall": {"pid": 103}}
        entries = []
        if not embedded:
            service["paired"] = {"pid": 101}
            entries += [(101, "paired-persistent-server", ["/opt/recall/bin/recall-surreal", "start", "--bind", "127.0.0.1:43131", "file:" + root + "/paired/database.db"], None),
                        (102, "explicit-fresh-schema", ["/opt/recall/bin/elephant", "migrate"], 0)]
        entries += [(103, "recall-authenticated-service", ["/opt/recall/bin/elephant", "serve", "--server-only", "--shutdown-timeout-seconds", "60"], None)]
        files, records = {}, []
        for index, (pid, purpose, command, exit_code) in enumerate(entries, 1):
            logs = [root + f"/logs/{index:02}.{suffix}" for suffix in ("stdout", "stderr")]
            for path in logs:
                files[path] = b"actual original command log"
            records.append({"index": index, "pid": pid, "purpose": purpose, "command": command,
                            "cwd": root + "/service", "environment_names": ["PATH"], "logs": logs,
                            "started_monotonic_ns": 10 + index, "exit_code": exit_code,
                            "log_sha256": {path: contract.sha(files[path]) for path in logs} if exit_code is not None else None})
        journal = {"format": "recall-runtime-process-commands/v1", "processes": records}
        command_path = root + "/process-commands.json"
        files[command_path] = contract.encode(journal)
        receipt = {"started_monotonic_ns": 10, "observed_monotonic_ns": 30, "processes": service,
                   "process_commands": {"path": command_path, "sha256": contract.sha(files[command_path])},
                   "schema_operation": {"kind": "initialize_runtime" if embedded else "migrate",
                                        "capture_index": 99 if embedded else None, "process_index": None if embedded else 2}}
        return helper, plan, receipt, journal, files

    def test_declared_migration_requires_real_original_command_and_complete_logs(self):
        helper, plan, receipt, journal, files = self.command_fixture()
        read = lambda path, bound: files[path]
        startup.startup_execution(receipt, plan, helper, {}, read)
        original = copy.deepcopy(journal)
        for change in ("command", "exit", "live", "logs"):
            changed = copy.deepcopy(original)
            if change == "command":
                changed["processes"][1]["command"] = ["/opt/recall/bin/elephant", "status"]
            elif change == "exit":
                changed["processes"][1]["exit_code"] = 1
            elif change == "live":
                changed["processes"][-1]["command"][-1] = "600"
            else:
                changed["processes"][1]["log_sha256"][changed["processes"][1]["logs"][0]] = "0" * 64
            raw = contract.encode(changed)
            files[receipt["process_commands"]["path"]] = raw
            receipt["process_commands"]["sha256"] = contract.sha(raw)
            with self.subTest(change=change), self.assertRaises(ValueError):
                startup.startup_execution(receipt, plan, helper, {}, read)

    def test_declared_embedded_initialization_requires_original_operator_wire_success(self):
        helper, plan, receipt, journal, files = self.command_fixture(embedded=True)
        root = plan["run_directory"]
        value = {"configured": True, "storage_ready": True, "backend": "elephant-db"}
        reply = {"jsonrpc": "2.0", "id": 99, "result": {"content": [{"type": "text", "text": json.dumps(value)}], "isError": False}}
        request = {"method": "POST", "path": "/mcp/full", "port": 43130, "credential_role": "operator", "space": None,
                   "body": {"id": 99, "method": "tools/call", "params": {"name": "initialize_runtime", "arguments": {
                       "backend": "elephant-db", "snapshot_path": root + "/service/state.snapshot", "ingest_enabled": False,
                       "extractor_required": False, "multimodal_asset_root": root + "/media"}}}}
        response = {"status": 200, "framing_complete": True, "error_type": None, "body": reply}
        read = lambda path, bound: files[path]
        startup.startup_execution(receipt, plan, helper, {99: (request, response, contract.encode(reply))}, read)
        sse = b"event: message\ndata: " + contract.encode(reply) + b"\n\n"
        streamed = {**response, "response_headers": {"content-type": ["text/event-stream"]}}
        startup.startup_execution(receipt, plan, helper, {99: (request, streamed, sse)}, read)
        for changed in ({**response, "framing_complete": False}, {**response, "error_type": "TimeoutError"},
                        {**response, "body": {**reply, "id": 100}}):
            with self.assertRaises(ValueError):
                startup.startup_execution(receipt, plan, helper, {99: (request, changed, contract.encode(reply))}, read)
        with self.assertRaises(ValueError):
            startup.startup_execution(receipt, plan, helper, {99: ({**request, "credential_role": "service-a"}, response, contract.encode(reply))}, read)

    def test_collector_cannot_adopt_a_live_unrelated_service_or_bootstrap(self):
        bootstrap = {"pid": 20, "ppid": 10, "pgrp": 10, "session": 10, "start_ticks": 100}
        service = {"pid": 30, "ppid": 20, "process_group": 30, "session": 30, "start_ticks": 110,
                   "executable": "/opt/recall/bin/elephant"}
        live = {20: bootstrap, 30: {"pid": 30, "ppid": 20, "pgrp": 30, "session": 30, "start_ticks": 110}}
        gateway = SimpleNamespace(process_identity=lambda pid: live[pid])
        receipt = {"bootstrap_process": bootstrap, "processes": {"recall": service}}
        with patch.object(startup.os, "getppid", return_value=20), patch.object(startup.os, "readlink", return_value=service["executable"]):
            startup.live_ancestry(receipt, {"process": {"pid": 10}}, gateway)
            live[30] = {**live[30], "start_ticks": 111}
            with self.assertRaises(ValueError):
                startup.live_ancestry(receipt, {"process": {"pid": 10}}, gateway)
            with self.assertRaises(ValueError):
                startup.live_ancestry(receipt, {"process": {"pid": 11}}, gateway)


if __name__ == "__main__":
    unittest.main()
