"""Pure startup contracts: no credentials, processes, sockets or provider calls."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
from pathlib import PurePosixPath
import re
import uuid

FORMAT = "recall-runtime-bootstrap/v1"
SCOPE = "authenticated-clean-persistent-startup-only"
BACKENDS = ("elephant-db", "surreal-db")
PRIMARY_BACKEND_SECONDS = 2700
D28_WHOLE_SECONDS = 7800
D28_ALLOCATION_SHA256 = "0a54fcd4c6359e39611567a06857f2d61975b3f19a80052f6ea49691f6d1633f"
SEMANTIC140_ALLOCATION_SHA256 = "e42df2638349fea37f94eb7f9ea79d2afad170a2d36a655869a40f25f83dcd01"
SEMANTIC140_PROFILE_ID = "semantic140-v1"
SEMANTIC140_ANCHOR_FORMAT = "recall-gateway-launch-anchor/v2"


def token_lifetime(remaining_seconds, *, management):
    # Tokens can outlive neither original run bound. This is only expiry, not a
    # delegation, provider allocation or permission to start another phase.
    require(type(remaining_seconds) in (int, float) and math.isfinite(remaining_seconds)
            and remaining_seconds > 0 and type(management) is bool, "original token lifetime unavailable")
    maximum = 3600 if management else 31 * 24 * 3600
    return min(maximum, math.ceil(remaining_seconds))

LIMITS = {"bootstrap_seconds": 300, "request_seconds": 15, "request_bytes": 131072,
          "response_bytes": 262144, "capture_bytes": 16777216, "requests": 160,
          "processes": 16, "log_bytes_per_stream": 4194304, "cleanup_seconds": 75}
CORE_SCOPES = {"read:status", "read:space_config", "read:source", "write:source",
               "read:doc", "read:doc:metadata", "read:doc:raw", "write:doc",
               "read:entity", "read:assertion", "read:relationship"}
CONDITIONAL_SCOPES = {"read:reprocess_job", "write:reprocess_job", "manage:wiki"}
BOOTSTRAP_SCOPES = ("read:space_config", "write:space_config", "manage:grant", "manage:provider")
SPACE_FIELDS = {"name", "security", "embedding_provider", "embedding_model", "embedding_dims",
                "purpose", "skill", "processing"}
PRIMARY_SPACE_POLICIES = {"semantics-household": ("synthetic-household", 3),
                          "semantics-business": ("synthetic-business", 7)}
WORKLOAD_POLICIES = frozenset({("household-policy", 3), ("business-policy", 7)})
IDENTITIES = ("source_commit", "artifact_sha256", "paired_artifact_sha256", "runtime_sha256",
              "configuration_sha256", "backend", "endpoint", "run_nonce")
CHECKS = {"unauthenticated_readiness", "missing_grant", "named_config_readback",
          "ordinary_config_readback", "ordinary_empty_inventory", "wrong_space",
          "bootstrap_data_denial", "read_only_write_denial", "grant_readback",
          "grant_revocation", "grant_restoration", "ready", "final_empty_inventory"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def encode(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def fields(value, required, optional=()):
    require(type(value) is dict and set(required) <= set(value)
            and set(value) <= set(required) | set(optional), "contract fields differ")
    return value


def worker_recipe_observation(value, backend, request_index, previous=None):
    """Validate an actual managed construction, shared by both startup producers."""
    require(type(value) is dict and type(value.get("managed_worker_recipe")) is dict,
            "actual managed worker construction recipe is unavailable")
    recipe = value["managed_worker_recipe"]
    fields(recipe, {"format", "instance_id", "backend", "work_queue", "embed_worker"})
    require(recipe["format"] == "recall-managed-worker-recipe/v1"
            and recipe["backend"] == backend and type(recipe["instance_id"]) is str
            and str(uuid.UUID(recipe["instance_id"])) == recipe["instance_id"]
            and type(recipe["work_queue"]) is dict and type(recipe["embed_worker"]) is dict,
            "actual managed worker recipe identity differs")
    observed = {"request_index": request_index, "instance_id": recipe["instance_id"],
                "recipe_sha256": sha(encode(recipe))}
    if previous is not None:
        require(observed["instance_id"] == previous["instance_id"]
                and observed["recipe_sha256"] == previous["recipe_sha256"],
                "original managed worker construction changed")
    return observed


def embedding_recipe_observation(name, value, config, request_index):
    """Bind actual factory observations to the original approved named config."""
    processing = config["processing"]
    recipes = value.get("embedding_execution_recipes")
    require(value.get("space") == name
            and value.get("processing_generation") == processing["generation"]
            and type(recipes) is dict
            and {"query_embedding", "document_embedding"} <= set(recipes)
            and set(recipes) <= {"query_embedding", "document_embedding", "reindex_embedding"},
            "actual named-space embedding recipe is unavailable or changed")
    for operation, recipe in recipes.items():
        require(type(recipe) is dict and type(recipe.get("provenance")) is dict,
                "actual embedding recipe provenance missing")
        binding = processing["bindings"].get(operation)
        require(type(binding) is dict and recipe["provenance"] == {
            "config": {"space_id": name, "config_id": config["id"],
                       "generation": processing["generation"], "policy_id": processing["policy_id"]},
            "operation": operation, "binding_id": binding["id"], "profile_id": binding["profile_id"]},
            "actual embedding recipe differs from original approved binding")
        profile = processing["profiles"][binding["profile_id"]]
        partition = recipe.get("partition")
        require(type(partition) is dict and partition.get("space_id") == name
                and partition.get("profile_id") == profile["id"]
                and partition.get("preprocessing") == profile["preprocessing"],
                "actual embedding recipe partition differs")
    return {"request_index": request_index, "recipes_sha256": sha(encode(recipes))}


def strict_json(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            require(key not in value, "duplicate JSON field")
            value[key] = item
        return value
    def finite(value):
        number = float(value)
        require(math.isfinite(number), "nonfinite JSON number")
        return number
    return json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_float=finite,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON number")))


def utc(value):
    require(type(value) is str, "explicit expiration timestamp required")
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.tzinfo is not None, "explicit timestamp timezone required")
    return parsed


def hex_identity(value, width=64):
    require(type(value) is str and re.fullmatch(f"[a-f0-9]{{{width}}}", value), "exact immutable identity required")


def private_path(value):
    path = PurePosixPath(value)
    require(type(value) is str and str(path) == value and path.is_absolute()
            and ".." not in path.parts and value.startswith("/work/") and len(value) <= 512,
            "private handoff path must be canonical and inside /work")
    return path


def create_request(value):
    fields(value, SPACE_FIELDS)
    require(type(value["name"]) is str and re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", value["name"]),
            "explicit bounded named space required")
    security = fields(value["security"], {"policy_id", "level", "labels", "handling"})
    require(type(security["policy_id"]) is str and type(security["level"]) is int
            and security["labels"] == [] and security["handling"] == [],
            "original classified policy envelope differs")
    pair = (security["policy_id"], security["level"])
    require(pair == PRIMARY_SPACE_POLICIES[value["name"]] if value["name"] in PRIMARY_SPACE_POLICIES
            else pair in WORKLOAD_POLICIES, "original named policy envelope differs")
    require(value["embedding_provider"] == "openai" and type(value["embedding_dims"]) is int
            and 1 <= value["embedding_dims"] <= 65536 and type(value["embedding_model"]) is str
            and value["embedding_model"], "explicit embedding identity required")
    require(type(value["purpose"]) is str and 0 < len(value["purpose"]) <= 4096
            and (value["skill"] is None or type(value["skill"]) is str), "explicit extraction guidance required")
    processing = value["processing"]
    require(type(processing) is dict and processing.get("generation") == 1
            and type(processing.get("generation")) is int and processing.get("index_generation") == 0
            and type(processing.get("index_generation")) is int and processing.get("pending_reindex") is None
            and bool(processing.get("bindings")), "fresh explicit processing bindings required")
    require(len(encode(value)) <= LIMITS["request_bytes"], "space creation exceeds request bound")
    return value


def validate_plan(plan):
    fields(plan, {"format", *(set(IDENTITIES) - {"configuration_sha256"}), "expires_at", "spaces",
                  "service_scopes", "dependencies", "run_directory"}, {"scoped_wiki"})
    require(plan["format"] == "recall-runtime-bootstrap-plan/v1" and plan["backend"] in BACKENDS,
            "unsupported startup plan")
    for name in IDENTITIES[:4]:
        hex_identity(plan[name], 40 if name == "source_commit" else 64)
    require(re.fullmatch(r"[A-Za-z0-9_-]{16,64}", plan["run_nonce"]), "fresh bounded nonce required")
    require(plan["endpoint"] == "http://127.0.0.1:43130", "fixed private Recall endpoint required")
    utc(plan["expires_at"])
    private_path(plan["run_directory"])
    require(plan["run_directory"] == "/work/bootstrap-" + plan["run_nonce"] + "-" + plan["backend"],
            "one exclusive backend cohort directory required")
    require(type(plan["spaces"]) is list and len(plan["spaces"]) == 2, "two original named spaces required")
    for space in plan["spaces"]:
        create_request(space)
    names = {row["name"] for row in plan["spaces"]}
    policies = {(row["security"]["policy_id"], row["security"]["level"]) for row in plan["spaces"]}
    require(len(names) == 2 and (policies == WORKLOAD_POLICIES
            or (names == set(PRIMARY_SPACE_POLICIES) and policies == set(PRIMARY_SPACE_POLICIES.values()))),
            "original policy separation differs")
    scoped_wiki = "scoped_wiki" in plan
    if scoped_wiki:
        require(plan["scoped_wiki"] is True and names == set(PRIMARY_SPACE_POLICIES)
                and policies == set(PRIMARY_SPACE_POLICIES.values()),
                "scoped wiki requires the explicit original primary plan")
    scopes = plan["service_scopes"]
    conditional_scopes = CONDITIONAL_SCOPES | ({"write:entity"} if scoped_wiki else set())
    require(type(scopes) is list and all(type(item) is str for item in scopes)
            and len(scopes) == len(set(scopes)) and CORE_SCOPES <= set(scopes)
            and set(scopes) <= CORE_SCOPES | conditional_scopes, "service scope allocation differs")
    if scoped_wiki:
        require({"write:entity", "manage:wiki"} <= set(scopes),
                "scoped wiki requires both explicit conditional scopes")
    require(type(plan["dependencies"]) is dict and 1 <= len(plan["dependencies"]) <= 128,
            "reviewed transitive helper identities required")
    for path, checksum in plan["dependencies"].items():
        canonical = PurePosixPath(path)
        require(str(canonical) == path and not canonical.is_absolute() and ".." not in canonical.parts
                and path.startswith(("scripts/qualification/", "scripts/release/", "tests/recall_preview/",
                                     "tests/recall_preview_semantics/")), "dependency outside reviewed runtime")
        hex_identity(checksum)
    return plan


def validate_config(request, actual, creator):
    # The API generates these fields. Their actual values are bound AFTER the
    # request, never invented in an immutable pre-execution expectation.
    fields(actual, {"id", "space_id", "created_at", "updated_at", "created_by",
                    "security", "subjects", "subjects_state", "provenance",
                    "embedding_provider", "embedding_model", "embedding_dims", "purpose", "processing"}, {"deleted_at", "skill"})
    require(type(actual["id"]) is str and re.fullmatch(r"space_config:[0-9a-f-]{36}", actual["id"]),
            "actual generated space identity missing")
    require(actual["space_id"] == request["name"] and actual["created_by"] == creator
            and actual.get("deleted_at") is None and actual["subjects"] == []
            and actual["subjects_state"] == "complete", "actual space envelope differs")
    utc(actual["created_at"])
    utc(actual["updated_at"])
    require(actual["created_at"] == actual["updated_at"], "fresh configuration has already changed")
    for name in SPACE_FIELDS - {"name"}:
        require(actual.get(name) == request[name], "actual named-space field differs: " + name)
    return actual


def process_identity(raw_stat, expected_pid, executable, executable_sha256):
    # comm can contain whitespace or ')': Linux's final ')' precedes the fields.
    prefix, separator, suffix = raw_stat.rpartition(") ")
    require(separator and prefix.split(" ", 1)[0] == str(expected_pid), "process identity differs")
    parts = suffix.split()
    require(len(parts) >= 20 and parts[0] != "Z" and parts[19].isdigit(), "live process start identity unavailable")
    require(int(parts[2]) == expected_pid and int(parts[3]) == expected_pid,
            "process is not its owned session/group leader")
    hex_identity(executable_sha256)
    return {"pid": expected_pid, "ppid": int(parts[1]), "process_group": int(parts[2]), "session": int(parts[3]),
            "start_ticks": int(parts[19]), "executable": executable, "executable_sha256": executable_sha256}


def denied(response, status, code):
    body = response["body"]
    require(response.get("status") == status and response.get("framing_complete") is True
            and response.get("error_type") is None, "denial was not a complete actual HTTP response")
    fields(body, {"error_code", "message", "retryable"})
    require(body["error_code"] == code and type(body["message"]) is str
            and body["retryable"] is False, "typed authorization denial differs")


def validate_receipt(receipt, plan, *, maximum_backend_seconds=PRIMARY_BACKEND_SECONDS):
    require(type(maximum_backend_seconds) is int
            and maximum_backend_seconds in (PRIMARY_BACKEND_SECONDS, D28_WHOLE_SECONDS),
            "unreviewed startup duration ceiling")
    required = {"format", "status", "qualification_scope", "qualified", "product_acceptance", *IDENTITIES,
                "authentication_enabled", "clean_persistent_store", "schema_explicitly_initialized",
                "schema_operation", "launch_context_sha256", "isolation_sha256", "plan_sha256",
                "allocation_started_monotonic_ns", "started_monotonic_ns", "observed_monotonic_ns",
                "original_deadline_monotonic_ns", "whole_deadline_monotonic_ns",
                "space_requests_sha256", "observed_configurations_sha256",
                "processes", "bootstrap_process", "process_commands", "checks", "captures",
                "configurations", "token_files", "runtime_config_sha256"}
    fields(receipt, required)
    require(receipt["format"] == FORMAT and receipt["status"] == "bootstrapped"
            and receipt["qualification_scope"] == SCOPE and receipt["qualified"] is False
            and receipt["product_acceptance"] is False, "startup observation is not product qualification")
    hex_identity(receipt["configuration_sha256"])
    for key in set(IDENTITIES) - {"configuration_sha256"}:
        require(receipt[key] == plan[key], "startup immutable identity differs: " + key)
    for key in ("authentication_enabled", "clean_persistent_store", "schema_explicitly_initialized"):
        require(receipt[key] is True, "actual startup proof is absent: " + key)
    for key in ("launch_context_sha256", "isolation_sha256", "plan_sha256", "runtime_config_sha256",
                "space_requests_sha256", "observed_configurations_sha256"):
        hex_identity(receipt[key])
    times = [receipt[key] for key in ("allocation_started_monotonic_ns", "started_monotonic_ns",
            "observed_monotonic_ns", "original_deadline_monotonic_ns", "whole_deadline_monotonic_ns")]
    require(all(type(value) is int and value > 0 for value in times) and times[0] <= times[1] <= times[2] < times[3] <= times[4]
            and times[3] - times[0] <= maximum_backend_seconds * 10**9,
            "original run clock was reset or exhausted")
    require(receipt["space_requests_sha256"] == sha(encode(plan["spaces"]))
            and receipt["observed_configurations_sha256"] == sha(encode(receipt["configurations"])),
            "space request or actual observation hash differs")
    names = {row["name"] for row in plan["spaces"]}
    require(set(receipt["configurations"]) == names and set(receipt["token_files"]) == names,
            "actual named configuration or ordinary credentials missing")
    require(type(receipt["checks"]) is dict and set(receipt["checks"]) == CHECKS,
            "startup proof family missing")
    require(type(receipt["captures"]) is list and 1 <= len(receipt["captures"]) <= LIMITS["requests"],
            "actual request/response captures missing")
    indices = set()
    for item in receipt["captures"]:
        fields(item, {"request_index", "request_path", "response_path", "request_sha256", "response_sha256"})
        require(type(item["request_index"]) is int and item["request_index"] > 0
                and item["request_index"] not in indices, "duplicate capture identity")
        indices.add(item["request_index"])
        for key in ("request_sha256", "response_sha256"):
            hex_identity(item[key])
        for key in ("request_path", "response_path"):
            private_path(item[key])
    for key, values in receipt["checks"].items():
        require(type(values) is list and values and all(type(i) is int and i in indices for i in values),
                "check lacks original capture reference: " + key)
    bootstrap = fields(receipt["bootstrap_process"], {"pid", "ppid", "pgrp", "session", "start_ticks"})
    require(all(type(value) is int and value > 0 for value in bootstrap.values()), "actual bootstrap process identity missing")
    commands = fields(receipt["process_commands"], {"path", "sha256"})
    private_path(commands["path"])
    hex_identity(commands["sha256"])
    expected_processes = {"recall"} | ({"paired"} if plan["backend"] == "surreal-db" else set())
    require(set(receipt["processes"]) == expected_processes, "actual owned service identities missing")
    for name, process in receipt["processes"].items():
        fields(process, {"pid", "ppid", "process_group", "session", "start_ticks", "executable", "executable_sha256"})
        require(all(type(process[k]) is int and process[k] > 0 for k in ("pid", "process_group", "session", "start_ticks"))
                and process["pid"] == process["process_group"] == process["session"]
                and process["ppid"] == bootstrap["pid"], "owned process group differs")
        require(process["executable"] == "/opt/recall/bin/" + ("elephant" if name == "recall" else "recall-surreal")
                and process["executable_sha256"] == plan["artifact_sha256" if name == "recall" else "paired_artifact_sha256"],
                "owned process executable differs")
    operation = receipt["schema_operation"]
    fields(operation, {"kind", "capture_index", "process_index"})
    require(operation["kind"] == ("initialize_runtime" if plan["backend"] == "elephant-db" else "migrate"),
            "schema initialization operation differs")
    if plan["backend"] == "elephant-db":
        require(type(operation["capture_index"]) is int and operation["capture_index"] in indices
                and operation["process_index"] is None, "actual embedded initialization capture absent")
    else:
        require(type(operation["process_index"]) is int and operation["process_index"] > 0
                and operation["capture_index"] is None, "actual explicit migration process absent")
    for name, path in receipt["token_files"].items():
        private_path(path)
        require(path == plan["run_directory"] + "/private/service-" + name + ".token", "ordinary token path changed")
    return receipt
