"""Owned network-namespace adapter for the reviewed container Graph fixtures.

Imported only after source/input hash review. No service, socket or provider is
started on import. The caller owns the original allocation and shared budgets.
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib.util
from pathlib import Path
import sys
import os
import stat
import time
from urllib.parse import quote

SHARED = ("full_workload_transport", "full_workload_spy", "full_workload_graph")
SYNTHETIC_KEY = "synthetic-controlled-provider-key"
GRAPH_SCOPES = ["read:status", "read:space_config", "read:source", "write:source",
                "read:doc", "read:doc:raw", "read:doc:metadata", "write:doc",
                "read:assertion", "read:entity", "read:rel", "read:search"]


def reviewed_modules(driver):
    result = {}
    for name in SHARED:
        path = driver.base_root / "tests/recall_preview_semantics" / (name + ".py")
        driver.require(path.is_file() and path.resolve() == path, "reviewed Graph component missing")
        prior = sys.modules.get(name)
        if prior is not None:
            driver.require(Path(prior.__file__).resolve() == path, "Graph module name already belongs to another input")
            result[name] = prior
            continue
        spec = importlib.util.spec_from_file_location(name, path)
        driver.require(spec is not None and spec.loader is not None, "reviewed Graph component cannot load")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        result[name] = module
    return result


def budget_module(driver):
    name = "container_graph_budget"
    path = driver.base_root / "scripts/qualification/container_graph_budget.py"
    spec = importlib.util.spec_from_file_location(name, path)
    driver.require(spec is not None and spec.loader is not None, "reviewed shared budget component missing")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class OwnedSpy:
    """Control G's actual spy in the original Recall container namespace."""
    def __init__(self, driver, helper, directory, recipe, nonce, deadline):
        self.driver, self.helper, self.directory = driver, helper, directory
        self.recipe, self.nonce, self.deadline = recipe, nonce, deadline
        self.rule_counts, self.held_rules = {}, {}
        self.failure, self.last = None, None
        self.closed = False
        self.refresh()

    def command(self, action, identifier=None):
        self.driver.owned.verify("container", self.helper, "graph-helper")
        arguments = ["exec", self.helper, "python3", "/opt/recall-graph-fixture/container_graph_helper.py",
                     "control", "--directory", "/fixture", "--nonce", self.nonce, "--command", action]
        if identifier is not None:
            self.driver.require(type(identifier) is int and identifier > 100000, "original physical call ID required")
            arguments += ["--physical-call-id", str(identifier)]
        return self.driver.commands.json(arguments, timeout=5, secret=True)

    def refresh(self):
        value = self.command("snapshot")
        listener = value["listener"]
        self.driver.require(listener.get("nonce") == self.nonce and listener.get("host") == "127.0.0.1"
                            and type(listener.get("port")) is int and 0 < listener["port"] <= 65535
                            and listener.get("endpoint") == f"http://127.0.0.1:{listener['port']}/v1",
                            "helper listener escaped original loopback identity")
        self.endpoint = listener["endpoint"]
        self.last = value["provider"]
        self.failure = self.last["error_type"]
        self.rule_counts = value["rule_counts"]
        self.held_rules = value["held_rules"]
        return self.last

    def snapshot_calls(self):
        return copy.deepcopy(self.last if self.closed else self.refresh())

    async def wait_held(self, rule_id):
        while time.monotonic() < self.deadline:
            self.refresh()
            self.driver.require(self.failure is None, "original remote spy failed")
            for identifier, rule in self.held_rules.items():
                if rule == rule_id:
                    return int(identifier)
            await asyncio.sleep(min(0.5, max(0.0, self.deadline - time.monotonic())))
        raise ValueError("original held-call deadline expired")

    def release(self, identifier):
        self.refresh()
        self.driver.require(str(identifier) in self.held_rules, "original remote call is no longer held")
        self.command("release", identifier)

    async def close(self):
        if self.closed:
            return
        try:
            self.command("close")
            _, output, _ = self.driver.commands.run(["wait", self.helper], timeout=10, secret=True)
            self.driver.require(output.strip() == b"0", "owned Graph helper did not finish cleanly")
            path = self.directory / "final.json"
            self.driver.require(path.resolve() == path and path.is_file() and path.stat().st_size <= 1024 * 1024,
                                "original final provider capture missing or unbounded")
            value = self.driver.base_decode(path.read_bytes())
            self.driver.require(value["listener"]["nonce"] == self.nonce, "final helper identity changed")
            self.last, self.rule_counts = value["provider"], value["rule_counts"]
            self.failure = self.last["error_type"]
        finally:
            self.closed = True
            self.driver.graph_budget.change(allow_expired=True)
            # The ordinary outer cleanup re-verifies all labels and removes the
            # helper even if its bounded control/close call failed.


def start_owned_spy(driver, recipe, recipe_sha256, limits, deadline):
    driver.require(driver.report.get("docker_root") is not None and os.getuid() != 0,
                   "original local daemon and unprivileged fixture owner required")
    image = driver.allocation["graph_helper_image_id"]
    images = driver.commands.json(["image", "inspect", image], secret=True)
    driver.require(len(images) == 1 and images[0].get("Id") == image and images[0].get("Os") == "linux"
                   and images[0].get("Architecture") == "amd64"
                   and not images[0].get("Config", {}).get("OnBuild")
                   and not images[0].get("Config", {}).get("Volumes"), "original helper image identity/shape differs")
    user = f"{os.getuid()}:{os.getgid()}"
    probe = driver.container("graph-helper-inventory", image,
        ["/opt/recall-graph-fixture/container_graph_helper.py", "inventory"], user=user, entrypoint="python3")
    driver.commands.run(["start", probe])
    _, exited, _ = driver.commands.run(["wait", probe], timeout=10, secret=True)
    driver.require(exited.strip() == b"0", "helper Python/source inventory failed")
    _, output, _ = driver.commands.run(["logs", probe], secret=True)
    inventory = driver.base_decode(output)
    paths = {"container_graph_helper.py": "scripts/qualification/container_graph_helper.py",
             "container_graph_budget.py": "scripts/qualification/container_graph_budget.py",
             "full_workload_transport.py": "tests/recall_preview_semantics/full_workload_transport.py",
             "full_workload_spy.py": "tests/recall_preview_semantics/full_workload_spy.py"}
    driver.require(type(inventory) is dict and set(inventory) == {"python_version", "source_sha256"}
                   and type(inventory["python_version"]) is list and len(inventory["python_version"]) == 2
                   and all(type(part) is int for part in inventory["python_version"])
                   and (3, 11) <= tuple(inventory["python_version"]) < (4, 0)
                   and inventory["source_sha256"] == {name: driver.base_digest((driver.base_root / path).read_bytes())
                                 for name, path in paths.items()}, "helper Python or copied reviewed source differs")
    server = driver.owned.verify("container", driver.server,
        next(purpose for kind, identity, purpose in driver.owned.resources if kind == "container" and identity == driver.server))
    network = driver.owned.verify("network", driver.network,
        next(purpose for kind, identity, purpose in driver.owned.resources if kind == "network" and identity == driver.network))
    driver.require(server.get("State", {}).get("Running") is True
                   and server.get("HostConfig", {}).get("NetworkMode") == driver.network
                   and network.get("Internal") is True and network.get("Driver") == "bridge",
                   "helper must join the exact running owned Recall internal namespace")
    directory = driver.root / "graph-fixture"
    directory.mkdir(mode=0o700)
    driver.require(directory.resolve() == directory and stat.S_IMODE(directory.stat().st_mode) == 0o700
                   and directory.stat().st_uid == os.getuid(), "private fixture bind custody differs")
    settings = {"format": "recall-container-graph-helper/v1", "nonce": driver.owned.nonce,
                "recipe": recipe, "recipe_sha256": recipe_sha256, "deadline": deadline, "limits": limits}
    raw = driver.base_encode(settings)
    driver.reserve_capture(len(raw) + 4096)
    driver.base_private_new(directory / "settings.json", raw)
    ledger = budget_module(driver).BudgetLedger.create(directory / "budget.json", driver.owned.nonce,
        calls=driver.total_http_calls, written=driver.evidence_capture_bytes, maximum_calls=1000,
        maximum_bytes=driver.evidence_cap, deadline_ns=int(deadline * 1e9))
    driver.graph_budget = ledger
    extra = ["--pull=never", "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges:true",
             "--pids-limit=64", "--memory=512m", "--memory-swap=512m", "--cpus=1", "--ulimit=core=0",
             "--log-driver=json-file", "--log-opt=max-size=1m", "--log-opt=max-file=2", "--no-healthcheck",
             "--user", user, "--network", "container:" + driver.server,
             "--mount", f"type=bind,source={directory},target=/fixture",
             "--env", "PYTHONDONTWRITEBYTECODE=1", "--env", "PYTHONUNBUFFERED=1",
             "--entrypoint", "python3", image, "/opt/recall-graph-fixture/container_graph_helper.py",
             "serve", "--directory", "/fixture"]
    helper = driver.owned.create("container", "graph-helper", extra)
    actual = driver.owned.verify("container", helper, "graph-helper")
    driver.require(actual.get("HostConfig", {}).get("NetworkMode") == "container:" + driver.server,
                   "helper did not join the original server namespace")
    driver.commands.run(["start", helper])
    spy = OwnedSpy(driver, helper, directory, recipe, driver.owned.nonce, deadline)
    driver.save("graph-listener-custody.json", {"helper_image_id": image, "helper_id": helper,
        "recall_container_id": driver.server, "internal_network_id": driver.network,
        "fixture_bind": str(directory), "endpoint": spy.endpoint, "source_inventory": inventory})
    return spy


class GraphClient:
    """G's exact client interface, retaining F's original transport/counters."""
    def __init__(self, clients, space):
        self.clients, self.space = clients, space
        self.deadline = clients.deadline_ns / 1e9
        self.clock = lambda: clients.clock_ns() / 1e9
        self.next_operation_poll = {}

    async def request(self, method, path, space, body=None, *, role):
        if space != self.space or role != "data":
            raise ValueError("Graph client cannot change its original named-space credential")
        if method == "GET" and path.startswith("/v1/operations/"):
            # Preserve the actual operation and overall deadline while avoiding
            # spending the whole 1000-call allocation during a real 30s retry.
            wait = max(0.0, self.next_operation_poll.get(path, 0) - self.clock())
            if wait:
                await asyncio.sleep(min(wait, max(0.0, self.deadline - self.clock())))
            self.next_operation_poll[path] = self.clock() + 0.5
        row = await self.clients.request("graph-public", method, path, "graph-service", space, body)
        return {**row, "error_type": row["transport_error"]}




def processing_for_endpoint(driver, configuration, endpoint):
    config = copy.deepcopy(configuration)
    connections = config["processing"]["connections"]
    driver.require(type(connections) is dict and 1 <= len(connections) <= 4,
                   "finite explicit Graph provider connections required")
    for connection in connections.values():
        driver.require(connection.get("endpoint") == "{owned_spy_endpoint}"
                       and connection.get("credential_ref") == "RECALL_BOUNDARY_SPY_KEY"
                       and connection.get("revoked") is False,
                       "Graph configuration must target only its original synthetic owned provider")
        connection["endpoint"] = endpoint
    return config


async def search_pair(driver, space, query):
    clients = driver.independent_clients()
    rest = await clients.request("graph-search-rest", "POST", f"/v1/spaces/{quote(space, safe='')}/search",
                                 "graph-service", space, query)
    mcp = await driver.graph_call_tool(clients, query, space)
    success, value = driver.graph_mcp_result(mcp)
    driver.require(rest["status"] == 200 and success, "actual Graph search failed")
    rest_value = driver.graph_complete_response(rest)
    driver.require(rest_value == value and type(value) is dict and set(value) == {"results", "coverage", "partial"},
                   "Graph HTTP/MCP search DTO differs")
    driver.require(value["partial"] is False, "completed Graph search is partial")
    for key, revision in query["source_revision_constraints"].items():
        coverage = value["coverage"][key]
        driver.require(coverage["state"] == "complete" and coverage["requested_revision"] == revision
                       and coverage["published_revision"] == revision and coverage["desired_revision"] == revision,
                       "Graph search lost original exact completed revision")
    return {"captures": [rest["capture"], mcp["capture"]], "response": value,
            "original_rest_body_bytes": rest["body_bytes"]}


def verify_evidence(driver, row, recipe, actual, *, claim):
    raw = recipe["source_request"]["doc"]["text"].encode()
    rendered = recipe["rendered_text"].encode()
    # These three independently reviewed literals preserve every raw byte and
    # add only the renderer's terminal LF. Refuse a different transform rather
    # than implementing a second generic renderer or searching for a quote.
    driver.require(rendered == raw + b"\n", "Graph literal transform changed")
    original = recipe["source_request"]["source"]
    immutable = actual["immutable_revision"]
    driver.require(hashlib.sha256(raw).hexdigest() == original["hash"]
                   and actual["source"]["hash"] == original["hash"]
                   and immutable["text"] == recipe["rendered_text"]
                   and immutable["text_hash"] == recipe["rendered_sha256"]
                   == hashlib.sha256(rendered).hexdigest(), "Graph original input/render identity differs")
    start, end = row.get("span", {}).get("start"), row.get("span", {}).get("end")
    render_start = row.get("rendered_span", {}).get("start")
    render_end = row.get("rendered_span", {}).get("end")
    driver.require(all(type(part) is int for part in (start, end, render_start, render_end))
                   and 0 <= start < end <= len(raw) and 0 <= render_start < render_end <= len(rendered)
                   and (start, end) == (render_start, min(render_end, len(raw))),
                   "Graph raw/rendered spans do not describe the original minimal cover")
    rendered_quote = rendered[render_start:render_end].decode("utf-8")
    driver.require(row.get("quote") == raw[start:end].decode("utf-8")
                   and row.get("rendered_quote_sha256") == hashlib.sha256(rendered_quote.encode()).hexdigest()
                   and row.get("rendered_text_sha256") == recipe["rendered_sha256"]
                   and row.get("source_id") == original["external_id"]
                   and row.get("revision") == recipe["source_request"]["intake"]["revision_id"]
                   and type(row.get("doc_id")) is str
                   and row["doc_id"] == immutable.get("doc") == actual["doc"]["id"]
                   and row.get("internal_doc_revision_id") == immutable["id"],
                   "Graph evidence changed original bytes, hashes, parent document or immutable revision")
    if claim:
        driver.require(any((render_start, render_end, rendered_quote) == (item["start"], item["end"], item["quote"])
                           for item in recipe["expected_assertions"]), "Graph selected claim quote was clipped or changed")


def verify_small_evidence(driver, value, recipes, published, arithmetic):
    originals = {recipe["source_request"]["source"]["external_id"]: (recipe, actual)
                 for recipe, actual in zip(recipes, published, strict=True)}
    claims = 0
    driver.require(len(value["results"]) == 10, "CB07 requires an actual new limit10 selection")
    for result in value["results"]:
        driver.require(result.get("kind") in ("claim", "chunk"), "CB07 selected an unexpected result family")
        claims += result["kind"] == "claim"
        evidence = result.get("evidence")
        driver.require(type(evidence) is list and evidence, "CB07 selected result lacks complete evidence")
        for row in evidence:
            driver.require(row.get("source_id") in originals, "CB07 evidence escaped original sources")
            recipe, actual = originals[row["source_id"]]
            verify_evidence(driver, row, recipe, actual, claim=result["kind"] == "claim")
    driver.require(claims >= 10 - arithmetic["limit20"]["maximum_selected_chunks"],
                   "CB07 actual selection differs from independent claim/chunk arithmetic")


async def run(driver, plan, inputs, *, modules=None):
    """Run actual public fixtures under one owned spy lifetime and allocation."""
    modules = modules or reviewed_modules(driver)
    graph = modules["full_workload_graph"]
    space = plan["space"]
    driver.require(space not in driver.cases["spaces"], "Graph phase cannot mutate original named-space baselines")
    clients = driver.independent_clients()
    client = GraphClient(clients, space)
    # The original public 270336-byte cap remains in IndependentClients. The
    # controlled provider's finite 128 * 3072 float batch needs <2MiB; these bytes
    # still share the same original 96MiB evidence and 1000-request allocation.
    limits = {"owned_spy_dispatches_per_backend": 1000, "owned_spy_request_bytes": 2_100_000,
              "owned_spy_response_bytes": 2 * 1024 * 1024, "owned_spy_concurrency": 16,
              "held_response_seconds": 120}
    spy = start_owned_spy(driver, inputs["spy"], plan["spy"]["canonical_sha256"], limits, client.deadline)
    driver.graph_spy = spy
    try:
        endpoint = spy.endpoint
        configuration = processing_for_endpoint(driver, plan["configuration"], endpoint)
        created = await clients.request("graph-create-space", "POST", "/v1/spaces", "operator", space, configuration)
        driver.require(created["status"] == 201 and created["transport_error"] is None,
                       "fresh exact Graph fixture space unavailable")
        driver.require(created["body"].get("space_id") == space
                       and all(created["body"].get(key) == value for key, value in configuration.items() if key != "name"),
                       "actual Graph configuration differs from the approved original")
        driver.save("graph-configuration.json", {"original_template": plan["configuration"],
                    "materialized": configuration, "created": created["body"]})
        granted = driver.http("PUT", f"/v1/spaces/{quote(space, safe='')}/grants/container-graph-service",
                              "operator", space, {"active": True, "scopes": GRAPH_SCOPES, "expected_generation": None})
        driver.require(granted["status"] == 200, "original Graph service grant unavailable")
        positive = graph.GraphFixture(client, spy, inputs["positive"], plan["positive"]["canonical_sha256"])
        completed = await positive.accept_and_wait()
        driver.save("graph-positive-publication.json", completed)
        constraints = {positive.request["source"]["external_id"]: positive.request["intake"]["revision_id"]}
        positive_query = {"query": plan["positive_query"], "ranking_mode": "lexical", "limit": 20,
                          "source_revision_constraints": constraints}
        actual_positive = await search_pair(driver, space, positive_query)
        positive_claims = 0
        for result in actual_positive["response"]["results"]:
            if result.get("kind") == "claim":
                driver.require(result.get("evidence"), "CC04 actual claim has no evidence")
                for row in result["evidence"]:
                    verify_evidence(driver, row, inputs["positive"], completed, claim=True)
                positive_claims += 1
        driver.require(positive_claims > 0, "CC04 needs positive exact independently expected Graph evidence first")
        absent = await search_pair(driver, space, {**positive_query, "query": plan["empty_query"]})
        driver.require(absent["response"]["results"] == [], "CC04 absent token returned evidence")
        driver.boundary_case("CC04", lambda: {"positive": actual_positive, "absent": absent,
                                               "original_publication": completed["accepted"]})

        published = []
        for descriptor, recipe in zip(plan["response_budget"], inputs["response_budget"], strict=True):
            fixture = graph.GraphFixture(client, spy, recipe, descriptor["canonical_sha256"])
            publication = await fixture.accept_and_wait()
            driver.save("graph-budget-publication-" + str(len(published)) + ".json", publication)
            published.append(publication)
        arithmetic = inputs["arithmetic"]
        query = {"query": arithmetic["query"], "ranking_mode": arithmetic["ranking_mode"], "limit": 20,
                 "source_revision_constraints": arithmetic["source_revision_constraints"]}
        error = await driver.error_pair(f"/v1/spaces/{quote(space, safe='')}/search", query,
            "search_scoped", query, "payload_too_large", 413, False, space=space, role="graph-service")
        small = await search_pair(driver, space, {**query, "limit": 10})
        verify_small_evidence(driver, small["response"], inputs["response_budget"], published, arithmetic)
        driver.require(small["original_rest_body_bytes"] <= arithmetic["limit10"]["full_response_bytes_upper_bound"],
                       "actual limit10 response exceeds independent byte bound")
        driver.boundary_case("CB07", lambda: {"error": error, "small": small,
            "arithmetic": arithmetic, "old_single_262145_quote_recipe": "unmet: unsupported default extraction window"})

        # The already completed positive fixture proves indexing; revoke only
        # its current query binding, then observe both real transport refusals.
        path = "/v1/spaces/" + quote(space, safe="")
        before = driver.http("GET", path, "operator", space)
        driver.require(before["status"] == 200, "original query configuration missing")
        processing = copy.deepcopy(before["body"]["processing"])
        binding = processing["bindings"][plan["query_binding_id"]]
        driver.require(binding["revoked"] is False, "query approval was never positive")
        binding["revoked"] = True
        changed = driver.http("PUT", path, "operator", space, {"processing": processing})
        driver.require(changed["status"] == 200
                       and changed["body"]["processing"]["bindings"][plan["query_binding_id"]]["revoked"] is True,
                       "actual query approval revocation failed")
        prior_calls = spy.snapshot_calls()["physical_connections"]
        refusal = await driver.error_pair(f"/v1/spaces/{quote(space, safe='')}/search",
            {**positive_query, "ranking_mode": "semantic"}, "search_scoped",
            {**positive_query, "ranking_mode": "semantic"}, "provider_unapproved", 403, False,
            space=space, role="graph-service")
        driver.require(spy.snapshot_calls()["physical_connections"] == prior_calls,
                       "revoked query approval dispatched another provider call")
        driver.boundary_case("CB09", lambda: {"refusal": refusal, "provider_calls_before": prior_calls,
            "provider_calls_after": spy.snapshot_calls()["physical_connections"],
            "original_publication": completed["accepted"], "revoked_configuration": changed["body"]})
    finally:
        await spy.close()
        driver.save("graph-provider-final.json", spy.snapshot_calls())
    driver.require(spy.failure is None, "actual controlled provider recorded a failure")
