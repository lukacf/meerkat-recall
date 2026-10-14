#!/usr/bin/env python3
"""Capture the frozen primary workload through an already running public API.

Inert by default. Does not launch services, create provider bindings, discover
credentials, retry submissions, score semantics, or certify gateway settlement.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
from pathlib import Path
import re
import sys
import time
import urllib.parse

# The reviewed gateway invokes Python with -I, which removes the script
# directory from import search. Resolve only these bundled sibling modules
# from this file's installed location, never the working directory/PYTHONPATH.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from primary_provider_driver_contract import (
    BACKENDS, LIMITS, MODES, acceptance, conditional_contract, corpus_contract,
    current_inventory, digest, encode, original_operation, publication_evidence, require, search_request,
    sha, source_request, stable_configuration, utc, rendered_contract, revision_evidence, RENDERED_MAP_SHA256,
    wiki_definition_evidence, wiki_publication_inputs, wiki_projection_evidence, scoped_wiki_plan_binding,
)
from primary_provider_driver_transport import PublicTransport, load_wire, private_new
from primary_provider_driver_startup import (C_HELPER, G_HELPER, read_private, validate_observations, revalidate_current_observations)
from primary_provider_driver_runtime import INSTALLED_VALIDATOR, VALIDATOR, validate_runtime_custody

ROOT = Path(__file__).resolve().parents[2]
DEPENDENCIES = (
    "scripts/qualification/primary_provider_driver.py",
    "scripts/qualification/primary_provider_driver_contract.py",
    "scripts/qualification/primary_provider_driver_transport.py",
    "scripts/qualification/primary_provider_driver_startup.py",
    "scripts/qualification/primary_provider_driver_runtime.py", C_HELPER, G_HELPER, VALIDATOR, INSTALLED_VALIDATOR,
    "scripts/qualification/runtime_package_bootstrap_evidence.py",
    "scripts/qualification/runtime_package_bootstrap_custody.py",
    "tests/recall_preview/dynamic-component-contract.json",
    "tests/recall_preview/DYNAMIC_COMPONENT_CONTRACT.md",
    "scripts/qualification/runtime_package_bootstrap.py",
    "scripts/qualification/runtime_package_bootstrap_io.py",
    "scripts/qualification/installed_process_capture.py",
    "tests/recall_preview/primary-rendered-map.json",
    "scripts/qualification/recall_workload.py", "scripts/qualification/installed_linux_offline.py",
    "scripts/qualification/recall_preview.py", "scripts/release/artifact.py",
    "tests/recall_preview/fixtures.py", "tests/recall_preview/matrix.py", "tests/recall_preview/oracle.py",
)
INPUTS = ("corpus", "expectations", "proposal", "configurations", "conditional_cases", "bootstrap_plan", "rendered_map", "runtime_review", "runtime_original_inputs")
OBSERVATIONS = ("runtime_receipt", "gateway_context", "runtime_custody")


def inventory():
    return {"format": "recall-primary-driver-inventory/v1", "limits": LIMITS,
            "dependencies": {name: sha((ROOT / name).read_bytes()) if (ROOT / name).is_file() else None for name in DEPENDENCIES},
            "backends": BACKENDS, "ranking_modes": MODES, "execution_authorized": False,
            "qualified": False, "provider_accounting": "requires separate actual gateway terminal settlement"}


def read_bound(path, maximum=16 * 1024 * 1024):
    require(path.is_absolute() and path == path.resolve() and path.is_file()
            and not path.is_symlink() and path.stat().st_size <= maximum, "bounded explicit regular input required")
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    require(len(raw) <= maximum, "input exceeded bound")
    return raw


def reviewed_inputs(path, parse_json):
    manifest = parse_json(read_bound(path))
    require(manifest.get("format") == "recall-primary-provider-run/v2"
            and manifest.get("execution_authorized") is True and manifest.get("provider_dispatch_authorized") is True,
            "run is not explicitly authorized")
    require(manifest["backend"] in BACKENDS and manifest["authentication_enabled"] is True,
            "explicit persistent backend and authenticated API required")
    require(dt.datetime.now(dt.timezone.utc) < utc(manifest["expires_at"]), "run authorization expired")
    require(re.fullmatch(r"[A-Za-z0-9_-]{16,64}", manifest["nonce"]), "fresh bounded run nonce required")
    utc(manifest["fixture_timestamp"])
    for name, width in (("source_commit", 40), ("artifact_sha256", 64), ("paired_artifact_sha256", 64), ("configuration_sha256", 64), ("runtime_sha256", 64)):
        require(re.fullmatch(f"[a-f0-9]{{{width}}}", manifest[name]), "exact installed identity absent")
    loaded = {}
    for name in (*INPUTS, "review"):
        reference = manifest["inputs"][name]
        require(Path(reference["path"]).is_relative_to("/inputs"), "immutable reviewed input mount required")
        raw = read_bound(Path(reference["path"]))
        require(sha(raw) == reference["sha256"], "reviewed input custody changed: " + name)
        loaded[name] = parse_json(raw)
        if name == "runtime_review":
            loaded["runtime_review_raw"] = raw
    review = loaded["review"]
    require(review.get("format") == "recall-primary-driver-review/v2" and review.get("approved") is True
            and review.get("reviewer") == "qualification (independent D lane)", "independent driver review absent")
    require(review["dependencies"] == inventory()["dependencies"] and all(review["dependencies"].values()),
            "reviewed driver/helper source changed or missing")
    for name in INPUTS:
        checksum = manifest["inputs"][name]["sha256"]
        require(review["inputs"][name] == checksum, "input was not reviewed: " + name)
    require(set(manifest["observations"]) == set(OBSERVATIONS), "exact dynamic startup observations required")
    for name in OBSERVATIONS:
        reference = manifest["observations"][name]
        raw = read_private(reference["path"], 8 * 1024**2)
        require(sha(raw) == reference["sha256"], "dynamic observation hash changed: " + name)
        loaded[name] = parse_json(raw)
    require(review["nonce"] == manifest["nonce"] and review["backend"] == manifest["backend"]
            and review["endpoint"] == manifest["endpoint"], "run approval does not bind this invocation")
    corpus_contract(loaded["corpus"], loaded["expectations"], loaded["proposal"])
    require(manifest["inputs"]["rendered_map"]["sha256"] == RENDERED_MAP_SHA256,
            "exact independent rendered-map bytes required")
    rendered_contract(loaded["rendered_map"], loaded["corpus"], manifest["inputs"]["corpus"]["sha256"])
    spaces = {row["id"] for row in loaded["corpus"]["spaces"]}
    require(set(loaded["configurations"]) == spaces and set(manifest["token_files"]) == spaces,
            "both exact named spaces required")
    cases = conditional_contract(loaded["conditional_cases"], spaces)
    scoped_wiki_plan_binding(cases, loaded["bootstrap_plan"])
    loaded["original_deadline"] = validate_observations(ROOT, manifest, loaded, review)
    loaded["runtime_component"] = validate_runtime_custody(ROOT, manifest, loaded, review)
    revalidate_current_observations(ROOT, manifest, loaded, review)
    for original in loaded["corpus"]["spaces"]:
        expected = loaded["configurations"][original["id"]]
        config = expected["configuration"]
        require(config["space_id"] == original["id"] and config["security"]["policy_id"] == original["policy"]
                and config["security"]["level"] == original["level"], "frozen space policy differs")
        require(expected["source_security"] == {"policy_id": original["policy"], "level": original["level"],
                                                "labels": [], "handling": []}, "source envelope differs")
        require(expected["guidance"] == original["guidance"], "frozen space guidance differs")
        require(config.get("purpose") == original["guidance"], "actual configured space guidance differs")
        proposed = next(row for row in loaded["proposal"]["spaces"] if row["id"] == original["id"])
        require(config["embedding_provider"] == "openai" and config["embedding_model"] == proposed["embedding_model"]
                and config["embedding_dims"] == proposed["dimensions"], "frozen embedding identity differs")
        require(isinstance(config["processing"], dict) and bool(config["processing"].get("bindings")),
                "explicit reviewed processing bindings required")
    return manifest, loaded


def synthetic_tokens(manifest):
    tokens = {}
    for space, name in manifest["token_files"].items():
        path = Path(name)
        require(path.is_relative_to("/work") and path.stat().st_mode & 0o077 == 0,
                "synthetic auth must be a private supplied /work file")
        token = read_bound(path, 16384).decode("ascii").strip()
        require(re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", token),
                "signed synthetic API token required")
        tokens[space] = token
    return tokens


def quote(value):
    return urllib.parse.quote(value, safe="")


def head_path(source):
    return f"/v1/spaces/{quote(source['space'])}/sources/{quote(source['id'])}/head"


class Primary:
    def __init__(self, manifest, loaded, transport):
        self.manifest, self.loaded, self.http = manifest, loaded, transport
        self.corpus = loaded["corpus"]
        self.acceptances, self.publications, self.failures, self.conditionals, self.queries = [], [], [], [], []
        self.current = {}
        self.rpc_id = 0

    async def configurations(self):
        for space, expected in self.loaded["configurations"].items():
            actual = await self.http.get("/v1/spaces/" + quote(space), space)
            require(stable_configuration(actual) == stable_configuration(expected["configuration"]),
                    "actual named-space configuration changed")

    async def inventory(self, space):
        rows, cursor, cursors = [], None, set()
        for _ in range(LIMITS["inventory_pages"]):
            path = f"/v1/spaces/{quote(space)}/sources?limit=100"
            if cursor is not None:
                path += "&cursor=" + quote(cursor)
            page = await self.http.get(path, space)
            require(isinstance(page.get("sources"), list), "public whole-space inventory absent")
            rows.extend(page["sources"])
            cursor = page.get("next_cursor")
            if cursor is None:
                keys = [row["external_source_id"] for row in rows]
                require(len(keys) == len(set(keys)), "duplicate inventory source identity")
                return rows
            require(isinstance(cursor, str) and cursor and cursor not in cursors, "inventory cursor loop")
            cursors.add(cursor)
        raise ValueError("whole-space inventory exceeds reviewed bounds")

    async def publish(self, source, deadline):
        key = (source["space"], source["id"])
        original = self.current.get(key)
        generation = None
        if original is not None:
            head = await self.http.get(head_path(source), source["space"])
            original_operation(original, head, complete=True)
            generation = original["generation"]
        body = source_request(source, self.manifest["backend"], self.manifest["nonce"],
                              self.manifest["fixture_timestamp"],
                              self.loaded["configurations"][source["space"]]["source_security"], generation)
        response = await self.http.request("POST", "/v1/sources/ingest", source["space"], body)
        accepted = acceptance(source, self.http.successful(response, 202))
        self.acceptances.append({"case": [*key, source["revision"]], "original": accepted,
                                 "request_index": response["request_index"]})
        while time.monotonic() < deadline:
            operation = await self.http.get("/v1/operations/" + quote(accepted["operation_id"]), source["space"])
            original_operation(accepted, operation)
            require(operation["state"] != "failed_final", "original publication failed terminally")
            if operation["state"] == "complete":
                original_operation(accepted, operation, complete=True)
                head = await self.http.get(head_path(source), source["space"])
                original_operation(accepted, head, complete=True)
                doc_path = "/v1/docs/" + quote(accepted["doc_id"])
                doc = await self.http.get(doc_path, source["space"])
                source_record = await self.http.get("/v1/sources/" + quote(accepted["internal_source_id"]), source["space"])
                require(source_record["current_ingest_mode"] == "graph" and source_record["lifecycle_status"] == "graph_ready"
                        and source_record["external_id"] == source["id"], "actual Graph publication absent")
                pipeline = await self.http.get(doc_path + "/pipeline-status", source["space"])
                nodes = await self.http.get(doc_path + "/chunks", source["space"])
                rendered = next(row for row in self.loaded["rendered_map"]["sources"]
                                if (row["space"], row["source_key"], row["revision"]) ==
                                   (source["space"], source["id"], source["revision"]))
                expected = self.loaded["configurations"][source["space"]]
                publication_evidence(source, accepted, doc, source_record, pipeline, nodes, expected, rendered)
                revision = await self.http.get(doc_path + "/revisions/" + quote(pipeline["revision"]), source["space"])
                revision_evidence(source, accepted, pipeline["revision"], revision, rendered,
                                  self.loaded["rendered_map"]["renderer"], expected)
                self.publications.append({"case": [*key, source["revision"]], "original": accepted,
                                          "operation": operation, "head": head, "pipeline": pipeline, "nodes": nodes,
                                          "doc_sha256": digest(doc), "source_sha256": digest(source_record),
                                          "immutable_revision_sha256": digest(revision),
                                          "private_finalizer_receipt": "unmet: not exposed by these public reads"})
                self.current[key] = accepted
                return
            await asyncio.sleep(min(1, max(0, deadline - time.monotonic())))
        raise TimeoutError("original publication deadline exhausted")

    async def ingest(self):
        deadline = min(self.http.deadline, time.monotonic() + LIMITS["publication_seconds"])
        blocked = set()
        for source in self.corpus["sources"]:
            key = (source["space"], source["id"])
            try:
                require(key not in blocked and time.monotonic() < deadline, "original source predecessor/deadline unavailable")
                await self.publish(source, deadline)
            except Exception as error:
                blocked.add(key)
                self.failures.append({"phase": "publication", "case": [*key, source["revision"]],
                                      "error_type": type(error).__name__, "harness_retries": 0})

    async def verify_current_inventory(self):
        for space in self.loaded["configurations"]:
            rows = await self.inventory(space)
            expected = current_inventory(self.corpus, space)
            require({row["external_source_id"]: row["desired_revision"] for row in rows} == expected,
                    "whole current space inventory differs from frozen original corpus")
            for row in rows:
                original_operation(self.current[(space, row["external_source_id"])], row, complete=True)

    async def rpc(self, space, method, params, timeout=15):
        self.rpc_id += 1
        request_id = self.rpc_id
        body = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
        response = await self.http.request("POST", "/mcp/maintenance", space, body, timeout, mcp_id=request_id)
        value = self.http.successful(response)
        require(value.get("jsonrpc") == "2.0" and value.get("id") == request_id and "error" not in value,
                "original MCP request failed or response identity changed")
        return value["result"]

    async def tool(self, space, name, args, timeout=15):
        result = await self.rpc(space, "tools/call", {"name": name, "arguments": args}, timeout)
        require(result.get("isError", False) is False, "actual MCP tool refused original request")
        if isinstance(result.get("structuredContent"), dict):
            value = result["structuredContent"]
        else:
            content = result.get("content", [])
            require(len(content) == 1 and content[0].get("type") == "text", "missing exact MCP tool payload")
            value = self.http.wire.helpers.capture.strict_json_body(content[0]["text"].encode())
        require(value.get("ok") is True and value.get("error") is None, "original tool outcome is not successful")
        return value["data"]

    async def conditional(self):
        cases = conditional_contract(self.loaded["conditional_cases"], set(self.loaded["configurations"]))
        initialized = set()
        for case in cases:
            captured = {"id": case["id"], "category": case["category"], "space": case["space"], "status": "incomplete"}
            # Initialization, preparation, the original mutation, and polling
            # consume one interval. A returned job never earns a fresh clock.
            deadline = min(self.http.deadline, time.monotonic() + LIMITS["conditional_seconds"])
            captured["original_deadline_monotonic"] = deadline

            def remaining(maximum=15):
                seconds = min(maximum, deadline - time.monotonic())
                require(seconds > 0, "original conditional deadline exhausted")
                return seconds

            async def call(name, args, maximum=15):
                result = await self.tool(case["space"], name, args, remaining(maximum))
                remaining()
                return result

            try:
                if case["space"] not in initialized:
                    await self.rpc(case["space"], "initialize", {"protocolVersion": "2025-03-26", "capabilities": {},
                                   "clientInfo": {"name": "primary-qualification", "version": "1"}}, remaining())
                    remaining()
                    initialized.add(case["space"])
                if case["tool"] == "compile_wiki_article":
                    await self.scoped_wiki(case, captured, call)
                    captured["status"] = "complete"
                    self.conditionals.append(captured)
                    continue
                value = await call(case["tool"], case["arguments"], LIMITS["conditional_seconds"])
                captured["original_result"] = value
                if case["category"] == "truth_maintenance":
                    job_id = value["id"]
                    require(isinstance(job_id, str) and job_id.startswith("reprocess_job:"), "actual original maintenance job absent")
                    while time.monotonic() < deadline:
                        progress = await call("get_reprocess_job_progress", {"id": job_id})
                        captured["progress"] = progress
                        require(progress["id"] == job_id, "maintenance original job substituted")
                        require(progress["status"] not in ("failed", "cancelled"), "original maintenance failed")
                        if progress["status"] == "completed":
                            captured["status"] = "complete"
                            break
                        await asyncio.sleep(min(1, remaining()))
                else:
                    reconciliation = value["reconciliation"]
                    require(value["managed"] is False and value.get("next_reconciliation_work_item") is None,
                            "unexpected recurring wiki work")
                    require(reconciliation["failed_articles"] == 0 and reconciliation["compiled_articles"] > 0,
                            "wiki lacks actually compiled and verified articles")
                    captured["status"] = "complete"
            except Exception as error:
                captured["error_type"] = type(error).__name__
            self.conditionals.append(captured)

    async def scoped_wiki(self, case, captured, call):
        require(case["definition"]["security"] == self.loaded["configurations"][case["space"]]["source_security"],
                "scoped wiki definition differs from original configured authority")
        originals = wiki_publication_inputs(case["space"], self.publications, self.current,
                                            self.loaded["rendered_map"])
        captured["preparation"] = []

        async def prepare(name, args):
            value = await call(name, args)
            captured["preparation"].append({"tool": name, "arguments": args, "original_result": value})
            return value

        definition = await prepare("upsert_knowledge_view_definition", case["definition"])
        definition_id = wiki_definition_evidence(case, definition)
        captured["original_definition_id"] = definition_id
        reread = await prepare("get_knowledge_view_definition", {"definition_id": definition_id})
        require(reread == definition, "scoped wiki original definition changed on readback")
        context = await prepare("get_knowledge_view", {"definition_id": definition_id, "freshness": "required", "limit": 64})
        captured["context_evidence"] = wiki_projection_evidence(case, definition, context, originals)
        arguments = {"definition_id": definition_id, "publish": True}
        captured["original_compile_arguments"] = arguments
        article = await call("compile_wiki_article", arguments, LIMITS["conditional_seconds"])
        captured["original_result"] = article
        captured["article_evidence"] = wiki_projection_evidence(case, definition, article, originals, context=context)
        captured["coverage"] = {"governed_article_public_receipts": "complete",
                                "writer_verifier_physical_dispatches": "unmet: join original gateway accounting",
                                "semantic_verdict": "unmet: independent review required",
                                "full_graph_planner": "unexercised", "full_graph_organizer": "unexercised"}

    async def search(self):
        for mode in MODES:
            for query in self.corpus["queries"]:
                row = {"id": query["id"], "query": query["query"], "space": query["space"], "ranking_mode": mode,
                       "fallback_fetches": 0, "query_rewrites": 0}
                try:
                    request = search_request(self.corpus, query, mode)
                    response = await self.http.request("POST", "/v1/spaces/" + quote(query["space"]) + "/search",
                                                       query["space"], request, LIMITS["request_seconds"])
                    row.update({"original_request": request, "capture": response, "response": response["body"]})
                    body = self.http.successful(response)
                    require(body.get("partial") is False and isinstance(body.get("results"), list)
                            and len(body["results"]) <= 3, "incomplete original ranking")
                    coverage = body.get("coverage", {})
                    for key, revision in current_inventory(self.corpus, query["space"]).items():
                        state = coverage.get(key, {})
                        require(state.get("state") == "complete" and state.get("requested_revision") == revision
                                and state.get("desired_revision") == revision and state.get("published_revision") == revision,
                                "whole current inventory coverage is incomplete")
                except Exception as error:
                    row["error_type"] = type(error).__name__
                self.queries.append(row)

    async def run(self):
        try:
            await self.configurations()
            for space in self.loaded["configurations"]:
                require(await self.inventory(space) == [], "fresh original corpus requires empty authenticated inventory")
            await self.ingest()
            try:
                await self.verify_current_inventory()
            except Exception as error:
                self.failures.append({"phase": "current-inventory", "error_type": type(error).__name__})
            await self.conditional()
            await self.search()
            await self.configurations()
        except (Exception, asyncio.CancelledError) as error:
            self.failures.append({"phase": "run", "error_type": type(error).__name__})
        complete = (len(self.publications) == 28 and len(self.queries) == 48 and not self.failures
                    and all("error_type" not in row for row in self.queries)
                    and all(row["status"] == "complete" for row in self.conditionals))
        observation = {"format": "recall-primary-provider-observation/v1", "backend": self.manifest["backend"],
                       "source_commit": self.manifest["source_commit"], "artifact_sha256": self.manifest["artifact_sha256"],
                       "configuration_sha256": self.manifest["configuration_sha256"], "corpus_sha256": digest(self.corpus),
                       "manifest_sha256": digest(self.manifest), "acceptances": self.acceptances, "publications": self.publications,
                       "conditional_cases": self.conditionals, "queries": self.queries, "failures": self.failures,
                       "http_requests": self.http.count, "captures": self.http.captures, "harness_retries": 0,
                       "fallback_fetches": 0, "query_rewrites": 0, "capture_complete": complete,
                       "qualified": False, "product_acceptance": False,
                       "provider_accounting": "unmet until actual G terminal-settlement-v2 closure is independently joined",
                       "startup_observations": self.manifest.get("observations", {}),
                       "runtime_component": self.loaded.get("runtime_component"),
                       "original_deadline_monotonic": self.http.deadline,
                       "per_query_provider_attribution": "unmet: public search does not expose physical attempt identity",
                       "private_finalizer_receipts": "unmet: public completion is not the private canonical receipt"}
        private_new(self.http.output / "observation.json", encode(observation))
        for mode in MODES:
            capture = {"manifest": {"evidence_class": "real-provider-unqualified", "backend": self.manifest["backend"],
                                    "query_mode": "semantic-only" if mode == "semantic" else "hybrid",
                                    "corpus_sha256": digest(self.corpus), "qualified": False},
                       "cases": [row for row in self.queries if row["ranking_mode"] == mode]}
            private_new(self.http.output / f"{mode}.json", encode(capture))
        return 0 if complete else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--execute-authorized", action="store_true")
    args = parser.parse_args()
    if not args.execute_authorized:
        print(encode(inventory()).decode())
        return 0
    require(args.manifest is not None, "explicit reviewed run manifest required")
    wire = load_wire(ROOT)
    manifest, loaded = reviewed_inputs(args.manifest, wire.helpers.capture.strict_json_body)
    wire.endpoint(manifest["endpoint"])
    output = Path(manifest["output_directory"])
    require(output.is_relative_to("/work"), "capture output must stay in the supplied private work mount")
    transport = PublicTransport(wire, manifest["endpoint"], synthetic_tokens(manifest), output,
                                loaded["original_deadline"])
    return asyncio.run(Primary(manifest, loaded, transport).run())


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        # No repr/traceback: tokens and private provider replies are never logged.
        raise SystemExit(78) from None
