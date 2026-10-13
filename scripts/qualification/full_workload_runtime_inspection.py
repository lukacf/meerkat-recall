"""Actual full-workload recipe captures on the original owned runtime and clock.

No process discovery, credentials, provider calls, expectation generation or
acceptance verdict lives here. The verified CLI supplies the original owner,
client and independently reviewed plan. Imports have no runtime side effects.
"""
from __future__ import annotations

import os
import re
import time

from runtime_package_bootstrap_contract import (
    embedding_recipe_observation, encode, fields, require, sha, strict_json,
    worker_recipe_observation,
)

BOUNDARIES = ("initial", "before_fw09_first_space", "before_fw09_second_space", "after_fw09_cleanup")
SPACES = ("qualification-household", "qualification-business")
QUEUE_FIELDS = {"enabled", "max_concurrent", "max_extract_concurrent", "work_item_timeout_secs",
                "transient_retry_max_attempts", "transient_retry_base_delay_secs",
                "transient_retry_max_delay_secs", "allowed_kinds", "disabled_kinds", "doc_tree"}
TREE_FIELDS = {"max_chunk_size", "chunk_overlap", "create_paragraphs", "detect_sections",
               "semantic_chunking", "create_summaries", "summary_threshold", "summarizer_model"}
EMBED_FIELDS = {"enabled", "batch_size", "provider", "model", "dimensions"}
FACTORY_FIELDS = {"max_total_http_attempts", "max_inputs_per_call", "initial_segment_bytes", "max_split_depth"}


def equal(actual, expected):
    """Canonical JSON preserves boolean versus integer distinctions."""
    return encode(actual) == encode(expected)


def subset(actual, expected):
    if type(expected) is dict:
        return type(actual) is dict and all(key in actual and subset(actual[key], value)
                                           for key, value in expected.items())
    return equal(actual, expected)


def policy_configuration(config):
    """Separate storage-owned content progress from approved processing policy."""
    require(type(config) is dict and type(config.get("processing")) is dict,
            "explicit original processing configuration required")
    processing = dict(config["processing"])
    epoch = processing.pop("index_generation", None)
    require(type(epoch) is int and 0 <= epoch <= 2**64 - 1,
            "actual nonboolean content epoch required")
    return {**config, "processing": processing}, epoch


def validate_expectations(expected):
    """No omitted field acquires an environment, default or observation value."""
    fields(expected, {"format", "work_queue", "embed_worker", "embedding_execution"})
    require(expected["format"] == "recall-full-workload-worker-expectations/v1",
            "explicit reviewed effective worker expectations required")
    queue, embed = expected["work_queue"], expected["embed_worker"]
    fields(queue, QUEUE_FIELDS)
    fields(queue["doc_tree"], TREE_FIELDS)
    fields(embed, EMBED_FIELDS)
    require(queue["enabled"] is True and embed["enabled"] is True, "both original worker loops required")
    for key in QUEUE_FIELDS - {"enabled", "allowed_kinds", "disabled_kinds", "doc_tree"}:
        require(type(queue[key]) is int and queue[key] > 0, "explicit positive queue setting required")
    # These are the independently frozen FW09 assumptions, not runtime defaults.
    require([queue[key] for key in ("max_concurrent", "max_extract_concurrent", "transient_retry_max_attempts",
                                  "transient_retry_base_delay_secs", "transient_retry_max_delay_secs")]
            == [1, 1, 3, 10, 120], "original FW09 single-worker retry assumptions differ")
    for key in ("allowed_kinds", "disabled_kinds"):
        require(type(queue[key]) is list and all(type(item) is str and item for item in queue[key])
                and len(set(queue[key])) == len(queue[key]), "explicit work-kind selection required")
    tree = queue["doc_tree"]
    for key in ("create_paragraphs", "detect_sections", "semantic_chunking", "create_summaries"):
        require(type(tree[key]) is bool, "explicit tree boolean required")
    for key in ("max_chunk_size", "chunk_overlap", "summary_threshold"):
        require(type(tree[key]) is int and tree[key] >= (0 if key == "chunk_overlap" else 1),
                "explicit bounded tree dimensions required")
    require(tree["chunk_overlap"] < tree["max_chunk_size"] and type(tree["summarizer_model"]) is str
            and tree["summarizer_model"], "explicit original tree recipe required")
    require(all(type(embed[key]) is int and embed[key] > 0 for key in ("batch_size", "dimensions"))
            and all(type(embed[key]) is str and embed[key] for key in ("provider", "model")),
            "explicit global embedding construction required")
    fields(expected["embedding_execution"], SPACES)
    for space in SPACES:
        operations = expected["embedding_execution"][space]
        fields(operations, {"query_embedding", "document_embedding"}, {"reindex_embedding"})
        for factory in operations.values():
            fields(factory, FACTORY_FIELDS)
            require(type(factory["max_inputs_per_call"]) is int and factory["max_inputs_per_call"] > 0,
                    "explicit factory batch bound required")
            for key in FACTORY_FIELDS - {"max_inputs_per_call"}:
                require(factory[key] is None or (type(factory[key]) is int and factory[key] > 0),
                        "explicit factory attempts and full-input limits required")
    return expected


def validate_factory(name, recipes, config, expected):
    """Check complete partitions and implementation limits, beyond lineage alone."""
    require(set(recipes) == set(expected), "actual original factory operation inventory differs")
    processing = config["processing"]
    for operation, recipe in recipes.items():
        fields(recipe, FACTORY_FIELDS | {"provenance", "partition"})
        require(equal({key: recipe[key] for key in FACTORY_FIELDS}, expected[operation]),
                "actual factory execution limits differ from reviewed input")
        binding = processing["bindings"][operation]
        profile = processing["profiles"][binding["profile_id"]]
        connection = processing["connections"][profile["connection_id"]]
        require(binding["revoked"] is False and connection["revoked"] is False
                and profile["id"] == processing["active_embedding_profile_id"],
                "current factory is not the original active approved binding")
        partition = {"space_id": name, "provider": connection["provider"],
                     "account_id": connection["account_id"], "endpoint": connection["endpoint"],
                     "profile_id": profile["id"], "model": profile["model"],
                     "model_version": profile["model_version"], "dimensions": profile["dimensions"],
                     "preprocessing": profile["preprocessing"]}
        require(equal(recipe["partition"], partition), "actual full vector partition differs")


class RuntimeRecipeInspector:
    def __init__(self, client, plan, owner):
        require(type(plan) is dict and plan.get("backend") in ("elephant-db", "surreal-db")
                and type(plan.get("run_nonce")) is str and plan["run_nonce"],
                "original full-workload backend and nonce required")
        require(type(plan.get("configurations")) is dict and set(plan["configurations"]) == set(SPACES),
                "both original reviewed named configurations required")
        self.plan = strict_json(encode(plan))
        self.expected = validate_expectations(self.plan.get("worker_runtime_expectations"))
        for name in SPACES:
            policy_configuration(self.plan["configurations"][name]["configuration"])
        require(callable(getattr(owner, "revalidate_original", None)), "actual original process owner required")
        require(getattr(owner, "client", None) is client, "inspection must share the original owned client")
        require(callable(getattr(client, "runtime_request", None))
                and "runtime:inspect" in client.tokens, "explicit original global runtime inspection token required")
        self.client, self.owner = client, owner
        self.original = None
        self.last_epochs = {}
        self.completed = []
        self.failed = False

    def check_clock(self):
        require(self.client.clock() < self.client.deadline and time.time() < self.client.expires,
                "original full-workload inspection clock expired")

    async def process_identity(self):
        self.check_clock()
        value = await self.owner.revalidate_original()
        self.check_clock()
        fields(value, {"format", "backend", "run_nonce", "processes"})
        require(value["format"] == "recall-full-workload-process-identity/v1"
                and value["backend"] == self.plan["backend"] and value["run_nonce"] == self.plan["run_nonce"],
                "actual original process allocation differs")
        fields(value["processes"], {"recall", "paired"})
        for name, process in value["processes"].items():
            if name == "paired" and self.plan["backend"] == "elephant-db":
                require(process is None, "embedded runtime unexpectedly has a paired process")
                continue
            fields(process, {"executable_sha256", "process"})
            require(type(process["executable_sha256"]) is str
                    and re.fullmatch(r"[0-9a-f]{64}", process["executable_sha256"]),
                    "original executable hash absent")
            fields(process["process"], {"pid", "ppid", "pgrp", "session", "start_ticks"})
            require(all(type(item) is int and item > 0 for item in process["process"].values()),
                    "actual original process identity absent")
        return strict_json(encode(value))

    @staticmethod
    def success(row, method, path, space, role):
        require(type(row) is dict and row.get("method") == method and row.get("path") == path
                and row.get("space") == space and row.get("credential_role") == role
                and type(row.get("id")) is int and row["id"] > 0
                and row.get("status") == 200 and row.get("framing_complete") is True
                and not row.get("error_type") and not row.get("connection_cleanup_failed"),
                "actual complete original inspection exchange required")
        require(type(row.get("body")) is dict, "actual inspection JSON object absent")
        return row["body"]

    async def runtime(self, *, initialize=False):
        identifier = self.client.count + 1
        body = {"jsonrpc": "2.0", "id": identifier,
                "method": "initialize" if initialize else "tools/call",
                "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                           "clientInfo": {"name": "full-workload-runtime-inspector", "version": "1"}}
                          if initialize else {"name": "get_runtime_config", "arguments": {}}}
        row = await self.client.runtime_request(body, response_limit=262144)
        envelope = self.success(row, "POST", "/mcp/full", None, "runtime:inspect")
        fields(envelope, {"jsonrpc", "id", "result"})
        require(envelope["jsonrpc"] == "2.0" and type(envelope["id"]) is int
                and envelope["id"] == identifier, "original runtime MCP response identity differs")
        result = envelope["result"]
        require(type(result) is dict, "actual runtime MCP result absent")
        if initialize:
            require(result.get("protocolVersion") == "2025-03-26", "runtime MCP protocol differs")
            return None, row["id"]
        require(result.get("isError") is not True and type(result.get("content")) is list
                and len(result["content"]) == 1, "one actual runtime tool result required")
        content = result["content"][0]
        require(type(content) is dict and content.get("type") == "text" and type(content.get("text")) is str,
                "actual runtime tool JSON text absent")
        value = strict_json(content["text"].encode())
        require(type(value) is dict, "actual runtime config object absent")
        return value, row["id"]

    async def named(self, name, suffix=""):
        path = "/v1/spaces/" + name + suffix
        row = await self.client.request("GET", path, name, role="manage", response_limit=262144)
        return self.success(row, "GET", path, name, name + ":manage"), row["id"]

    def retain(self, observation):
        """Small comparison index only; original HTTP/process bytes stay intact."""
        raw = encode(observation)
        require(len(raw) <= 65536, "inspection metadata exceeds original sub-bound")
        capture = self.client.capture
        capture.reserve(len(raw))
        path = capture.directory / ("runtime-inspection-" + observation["boundary"] + ".json")
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        capture.held -= len(raw)
        capture.written += len(raw)

    async def capture(self, boundary: str) -> None:
        require(not self.failed and len(self.completed) < len(BOUNDARIES)
                and boundary == BOUNDARIES[len(self.completed)], "original inspection boundary order differs")
        # A failed or cancelled capture cannot be retried to adopt a replacement.
        self.failed = True
        before = await self.process_identity()
        if self.original is not None:
            require(equal(before, self.original["processes"]), "original owned process changed")
        initialization_index = None
        if boundary == "initial":
            _, initialization_index = await self.runtime(initialize=True)
        value, index = await self.runtime()
        worker = worker_recipe_observation(value, self.plan["backend"], index,
                                           None if self.original is None else self.original["worker"])
        recipe = value["managed_worker_recipe"]
        require(equal(recipe["work_queue"], self.expected["work_queue"])
                and equal(recipe["embed_worker"], self.expected["embed_worker"]),
                "actual effective worker recipe differs from independent expectations")
        providers, configurations = {}, {}
        for name in SPACES:
            config, config_index = await self.named(name)
            expected_config = self.plan["configurations"][name]["configuration"]
            policy, epoch = policy_configuration(config)
            expected_policy, initial_epoch = policy_configuration(expected_config)
            require(config.get("space_id") == name and type(config.get("id")) is str
                    and config["id"].startswith("space_config:") and subset(policy, expected_policy)
                    and equal(policy["processing"], expected_policy["processing"]),
                    "actual named configuration differs from original reviewed input")
            require(epoch == initial_epoch if self.original is None else epoch >= self.last_epochs[name],
                    "original content epoch is missing, substituted or regressed")
            stable = {"id": config["id"], "space_id": name, "processing": policy["processing"]}
            configurations[name] = {"request_index": config_index, "sha256": sha(encode(stable)),
                                    "index_generation": epoch}
            status, status_index = await self.named(name, "/status")
            providers[name] = embedding_recipe_observation(name, status, config, status_index)
            validate_factory(name, status["embedding_execution_recipes"], config,
                             self.expected["embedding_execution"][name])
        after = await self.process_identity()
        require(equal(before, after), "original process changed across inspection requests")
        if self.original is not None:
            require(all(providers[name]["recipes_sha256"] == self.original["providers"][name]["recipes_sha256"]
                        and configurations[name]["sha256"] == self.original["configurations"][name]["sha256"]
                        for name in SPACES), "original approved configuration or factory changed")
        observation = {"format": "recall-full-workload-runtime-inspection/v1", "boundary": boundary,
                       "backend": self.plan["backend"], "run_nonce": self.plan["run_nonce"],
                       "processes": before, "worker": worker, "providers": providers,
                       "configurations": configurations, "initialization_request_index": initialization_index,
                       "expected_sha256": sha(encode(self.expected)), "product_acceptance": False}
        self.check_clock()
        self.retain(observation)
        if self.original is None:
            self.original = strict_json(encode(observation))
        self.last_epochs = {name: item["index_generation"] for name, item in configurations.items()}
        self.completed.append(boundary)
        self.failed = False
