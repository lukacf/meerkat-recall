"""Resolve independently pinned workload inputs without executing the workload.

The returned in-memory object is the L driver's existing case-plan format.
This module has no product, provider, socket, subprocess, or receipt producer.
"""
from __future__ import annotations

import hashlib
import json
import ipaddress
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

FILE_LIMIT = 8 * 1024 * 1024
PACKAGE_LIMIT = 32 * 1024 * 1024
SPACES = ("qualification-household", "qualification-business")
CASES = tuple(f"FW{number:02}" for number in range(1, 15))


class PlanError(ValueError):
    """An original input or required independent expectation is unavailable."""


def require(condition, message):
    if not condition:
        raise PlanError(message)


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON member: " + key)
        result[key] = value
    return result


def decode(raw):
    def nonfinite(value):
        raise PlanError("non-finite JSON number: " + value)
    try:
        return json.loads(raw, object_pairs_hook=unique_object, parse_constant=nonfinite)
    except (UnicodeError, json.JSONDecodeError, RecursionError) as error:
        raise PlanError("invalid original JSON input") from error


class Inputs:
    """Read only exact files below caller-supplied source checkout roots."""

    def __init__(self, roots):
        self.roots = {name: Path(path).resolve(strict=True) for name, path in roots.items()}
        self.bytes = 0
        self.observed = []

    def read(self, descriptor, *, json_value=True):
        require(type(descriptor) is dict and {"root", "path", "sha256"} <= set(descriptor),
                "complete independently pinned file descriptor required")
        relative = PurePosixPath(descriptor["path"])
        require(not relative.is_absolute() and relative.parts and
                all(part not in ("..", ".") for part in relative.parts), "unsafe input path")
        require(descriptor["root"] in self.roots, "required source root is absent")
        root = self.roots[descriptor["root"]]
        path = root.joinpath(*relative.parts).resolve(strict=True)
        require(path.is_relative_to(root) and path.is_file(), "input escaped its source root")
        # Read a finite prefix even when a reviewed file was replaced by a larger one.
        with path.open("rb") as source:
            raw = source.read(FILE_LIMIT + 1)
        require(len(raw) <= FILE_LIMIT, "original input exceeds eight MiB")
        self.bytes += len(raw)
        require(self.bytes <= PACKAGE_LIMIT, "source package exceeds bounded input capacity")
        require(sha(raw) == descriptor["sha256"], "original file hash changed: " + str(relative))
        if "bytes" in descriptor:
            require(type(descriptor["bytes"]) is int and descriptor["bytes"] == len(raw),
                    "original byte length changed")
        value = decode(raw) if json_value else None
        if "canonical_sha256" in descriptor:
            require(json_value and sha(encode(value)) == descriptor["canonical_sha256"],
                    "original canonical JSON hash changed")
        self.observed.append({"root": descriptor["root"], "path": str(relative), "resolved_path": str(path),
                              "sha256": sha(raw), "bytes": len(raw)})
        return value


def recipe_descriptor(bundle, entry):
    return {"root": "recipes", "path": bundle["recipe_directory"] + "/" + entry["path"],
            **{key: entry[key] for key in ("file_sha256", "canonical_sha256", "bytes")},
            "sha256": entry["file_sha256"]}


def worker_runtime_expectations(expected):
    """Retain complete independent worker inputs; never infer runtime defaults."""
    value = expected.get("worker_runtime_expectations")

    def fields(obj, names, label):
        require(type(obj) is dict and set(obj) == set(names),
                "complete explicit worker " + label + " required")

    def typed(obj, schema, label):
        fields(obj, schema, label)
        for name, kind in schema.items():
            item = obj[name]
            require(type(item) is kind, "invalid worker " + label + "." + name)
            if kind is int:
                require(item >= 0, "negative worker " + label + "." + name)
            elif kind is str:
                require(bool(item), "empty worker " + label + "." + name)
            elif kind is list:
                require(all(type(entry) is str and entry for entry in item) and
                        len(item) == len(set(item)), "invalid worker kind inventory")

    fields(value, ("format", "work_queue", "embed_worker", "embedding_execution"), "expectations")
    require(value["format"] == "recall-full-workload-worker-expectations/v1",
            "explicit worker expectation format required")
    typed(value["work_queue"], {
        "enabled": bool, "max_concurrent": int, "max_extract_concurrent": int,
        "work_item_timeout_secs": int, "transient_retry_max_attempts": int,
        "transient_retry_base_delay_secs": int, "transient_retry_max_delay_secs": int,
        "allowed_kinds": list, "disabled_kinds": list, "doc_tree": dict,
    }, "work_queue")
    typed(value["work_queue"]["doc_tree"], {
        "max_chunk_size": int, "chunk_overlap": int, "create_paragraphs": bool,
        "detect_sections": bool, "semantic_chunking": bool, "create_summaries": bool,
        "summary_threshold": int, "summarizer_model": str,
    }, "doc_tree")
    typed(value["embed_worker"], {
        "enabled": bool, "batch_size": int, "provider": str, "model": str, "dimensions": int,
    }, "embed_worker")
    fields(value["embedding_execution"], SPACES, "embedding spaces")
    for space in SPACES:
        factories = value["embedding_execution"][space]
        fields(factories, ("query_embedding", "document_embedding"), "embedding operations")
        for operation, limits in factories.items():
            typed(limits, {"max_total_http_attempts": int, "max_inputs_per_call": int,
                           "initial_segment_bytes": int, "max_split_depth": int}, operation)
    # Validation is structural. Qualification approves the actual literal values;
    # the runtime inspector compares every value with the original observations.
    return decode(encode(value))


def runtime_principal_expectations(expected):
    """Carry the original reviewed management and revocable input literals."""
    templates = expected.get("runtime_principal_templates")
    require(type(templates) is dict and set(templates) == set(SPACES),
            "both independent principal templates required")
    for roles in templates.values():
        require(type(roles) is dict and set(roles) == {"manage", "revocable"},
                "explicit original management and revocable principals required")
        for principal in roles.values():
            require(type(principal) is dict and set(principal) == {"subject", "scopes"} and
                    type(principal["subject"]) is str and principal["subject"] and
                    type(principal["scopes"]) is list and principal["scopes"] and
                    all(type(scope) is str and scope for scope in principal["scopes"]) and
                    len(principal["scopes"]) == len(set(principal["scopes"])),
                    "explicit original principal subject and scopes required")
    return decode(encode(templates))


def validate_population(contract, corpus, rendered, recipes):
    require(corpus["spaces"] == contract["spaces"] and len(corpus["sources"]) == 50,
            "original fifty-source corpus differs")
    originals = {}
    for source in corpus["sources"]:
        identity = (source["space"], source["key"])
        require(source["space"] in SPACES and identity not in originals and
                sha(source["text"].encode()) == source["sha256"], "original corpus source differs")
        originals[identity] = source
    require(all(sum(space == item[0] for item in originals) == 25 for space in SPACES),
            "original two-space distribution differs")
    maps = {(row["space"], row["source_key"]): row for row in rendered["sources"]}
    require(len(rendered["sources"]) == len(maps) == 50 and set(maps) == set(originals),
            "independent complete rendered map required")
    for identity, source in originals.items():
        row = maps[identity]
        require(row["revision"] == source["revision"] and row["input_sha256"] == source["sha256"]
                and sha(row["rendered_text"].encode()) == row["rendered_sha256"],
                "original source-specific rendered expectation differs")
    identities = set(originals)
    extras = {(space, key) for space in SPACES for key in ("full/large", "full/shared/a", "full/shared/b")}
    for recipe in recipes.values():
        request = recipe["source_request"]
        source, doc = request["source"], request["doc"]
        identity = (source["space_id"], source["external_id"])
        require(identity in set(originals) | extras, "Graph recipe introduces another source")
        require("expected_generation" not in request["intake"], "template invents runtime generation")
        require(len(doc["text"].encode()) <= 2_000_000 and
                sha(doc["text"].encode()) == source["hash"], "original Graph bytes differ")
        require(sha(recipe["rendered_text"].encode()) == recipe["rendered_sha256"],
                "original Graph rendered expectation differs")
        identities.add(identity)
    require(len(recipes) == 24 and identities == set(originals) | extras and len(identities) == 56,
            "original twenty-four Graph recipes and fifty-six source identities required")


def validate_load(plan):
    require(set(plan) == {"format", "requests"} and
            plan["format"] == "recall-full-workload-lexical-load/v1", "original lexical plan required")
    rows = plan["requests"]
    require(type(rows) is list and len(rows) == 280, "exactly 280 original load intents required")
    identities, coordinates = set(), set()
    for row in rows:
        require(set(row) == {"id", "caller", "ordinal", "space", "body", "expected_coverage"},
                "load request fields differ")
        require(type(row["id"]) is str and row["id"] and row["id"] not in identities,
                "original load identity missing or duplicated")
        coordinate = (row["caller"], row["ordinal"])
        require(type(row["caller"]) is int and 0 <= row["caller"] < 140 and
                type(row["ordinal"]) is int and row["ordinal"] in (1, 2) and
                coordinate not in coordinates, "original caller/ordinal differs")
        identities.add(row["id"])
        coordinates.add(coordinate)
        require(row["space"] in SPACES, "load space differs")
        body = row["body"]
        require(set(body) == {"query", "ranking_mode", "limit", "source_revision_constraints"}
                and type(body["query"]) is str and 0 < len(body["query"].encode()) <= 8192
                and body["ranking_mode"] == "lexical" and type(body["limit"]) is int
                and 1 <= body["limit"] <= 50, "load intent is not the explicit bounded lexical request")
        constraints = body["source_revision_constraints"]
        require(type(constraints) is dict and 0 < len(constraints) <= 56 and
                set(row["expected_coverage"]) == set(constraints), "exact coverage inventory required")
        for key, revision in constraints.items():
            require(type(key) is str and key and type(revision) is str and revision and
                    row["expected_coverage"][key] == {"requested_revision": revision,
                    "published_revision": revision, "desired_revision": revision, "state": "complete"},
                    "independent complete load coverage differs")


def validate_cases(cases, recipes):
    required = {"FW03": {"revision"}, "FW04": {"shared_recipes", "actions"},
                "FW05": {"source_labels"}, "FW06": {"recipe", "pending_query", "completed_query"},
                "FW07": {"recipe", "idempotency_key", "hidden_query", "original_terminal"},
                "FW08": {"recipes"}, "FW09": {"running_recipe", "pending_recipe", "failed_recipe",
                "intake_recipe", "observe_seconds", "queries"},
                "FW10": {"space", "source_label", "subject", "role"},
                "FW11": {"recipe", "revoke_kind", "revoke_id", "observe_seconds", "original_failure"},
                "FW12": {"large_graph_recipes"}}
    for identifier, keys in required.items():
        rows = cases[identifier].get("spaces") if "FW06" <= identifier <= "FW11" else [cases[identifier]]
        if "FW06" <= identifier <= "FW11":
            require(type(rows) is list and len(rows) == 2 and
                    {row.get("space") for row in rows} == set(SPACES), "both space cases required: " + identifier)
        for row in rows:
            require(type(row) is dict and keys <= set(row), "required independent case fields missing: " + identifier)
            if "observe_seconds" in row:
                require(type(row["observe_seconds"]) is int and 0 < row["observe_seconds"] <= 120,
                        "finite original lifecycle observation cap required")
            references = [row[key] for key in ("recipe", "running_recipe", "pending_recipe", "failed_recipe", "intake_recipe") if key in row]
            for key in ("recipes", "shared_recipes", "large_graph_recipes"):
                if key in row:
                    require(type(row[key]) is list, "explicit recipe inventory required")
                    references.extend(row[key])
            for key in references:
                require(type(key) is str and key in recipes, "case refers to missing independent recipe")
                if "space" in row:
                    require(recipes[key]["source_request"]["source"]["space_id"] == row["space"],
                            "case recipe is in a different space")
    require(len(cases["FW04"]["shared_recipes"]) == 4 and
            len(cases["FW12"]["large_graph_recipes"]) == 2 and
            len(cases["FW05"]["source_labels"]) >= 2, "original shared/large/no-op inventory differs")


def validate_load_evidence(plan, requirements, requirements_sha256):
    """Bind each original positive load intent to an independent full quote."""
    require(type(requirements_sha256) is str and len(requirements_sha256) == 64 and
            all(char in "0123456789abcdef" for char in requirements_sha256) and
            sha(encode(requirements)) == requirements_sha256,
            "original load evidence map hash differs")
    require(type(requirements) is dict and set(requirements) == {row["id"] for row in plan["requests"]},
            "all 280 original request identities need evidence requirements")
    for row in plan["requests"]:
        expected = requirements[row["id"]]
        require(type(expected) is dict and set(expected) == {"required_evidence", "required_claim_evidence"},
                "exact load evidence expectation fields required")
        ordinary, claims = expected["required_evidence"], expected["required_claim_evidence"]
        require(type(ordinary) is list and ordinary and type(claims) is list,
                "known-positive load requests cannot omit original evidence")
        for value in ordinary + claims:
            require(type(value) is dict and set(value) == {"source_key", "revision", "quote", "quote_hash"}
                    and all(type(item) is str and item for item in value.values()), "full original literal evidence required")
            require(row["body"]["source_revision_constraints"].get(value["source_key"]) == value["revision"]
                    and sha(value["quote"].encode()) == value["quote_hash"],
                    "load evidence belongs to another request or has changed bytes")
        require(all(value in ordinary for value in claims), "claim requirement must retain the same original evidence")


def configuration_requests(expectations, owned_endpoints):
    """Bind only the owner's actual isolated spy endpoints to input templates.

    This returns requests for the existing configuration owner. It neither sends
    them nor asserts startup, current grants, zero egress, or allocation custody.
    The endpoint binding and resulting request hash must be retained separately.
    """
    require(set(owned_endpoints) == set(SPACES), "both original owned endpoints required")
    templates = expectations["space_request_templates"]
    require(set(templates) == set(SPACES), "both original configuration templates required")
    result = {}
    for space in SPACES:
        value = owned_endpoints[space]
        require(type(value) is str, "actual endpoint must be a string")
        parsed = urlsplit(value)
        try:
            address = ipaddress.ip_address(parsed.hostname or "")
            port = parsed.port
        except ValueError as error:
            raise PlanError("actual owned IPv4 loopback endpoint required") from error
        require(parsed.scheme == "http" and address.version == 4 and address.is_loopback and
                port is not None and 1 <= port <= 65535 and parsed.path in ("", "/v1") and
                parsed.username is None and parsed.password is None and not parsed.query and
                not parsed.fragment, "actual endpoint is outside the isolated spy contract")
        request = decode(encode(templates[space]))
        require(request["name"] == space and set(request["processing"]["connections"]) == {"owned-spy"},
                "original configuration connection identity differs")
        connection = request["processing"]["connections"]["owned-spy"]
        require("endpoint" not in connection, "template must not invent an actual allocated endpoint")
        connection["endpoint"] = value
        result[space] = request
    return result


def runtime_plan_document(expectations, runtime_inputs, spy_recipe):
    """Bind reviewed source inputs to an explicit pre-start runtime document.

    The original allocation owner supplies and pins all runtime_inputs. This
    function creates no paths, processes, tokens, grants, observations or
    execution authority; the runtime owner validates those actual resources.
    """
    fields = {"backend", "run_nonce", "endpoint", "spy_endpoint", "run_directory", "archive",
              "artifact_sha256", "binary_sha256", "source_commit", "target", "paired", "spy_credential"}
    require(type(runtime_inputs) is dict and set(runtime_inputs) == fields |
            ({"spy_credentials"} if "spy_credentials" in runtime_inputs else set()),
            "complete original pre-start runtime inputs required")
    require(runtime_inputs["backend"] in ("elephant-db", "surreal-db"), "supported original backend required")
    require(all(type(runtime_inputs[key]) is str and runtime_inputs[key] for key in fields - {"paired"}),
            "explicit original runtime values required")
    for key, size in (("artifact_sha256", 64), ("binary_sha256", 64), ("source_commit", 40)):
        require(len(runtime_inputs[key]) == size and all(c in "0123456789abcdef" for c in runtime_inputs[key]),
                "invalid original runtime digest: " + key)
    for key in ("run_directory", "archive"):
        path = PurePosixPath(runtime_inputs[key])
        require(path.is_absolute() and str(path) == runtime_inputs[key] and ".." not in path.parts,
                "canonical absolute original runtime path required")
    paired = runtime_inputs["paired"]
    if runtime_inputs["backend"] == "elephant-db":
        require(paired is None, "embedded allocation cannot adopt a paired process")
    else:
        require(type(paired) is dict and set(paired) == {"binary", "binary_sha256", "endpoint"} and
                all(type(item) is str and item for item in paired.values()),
                "original reviewed paired executable and endpoint required")
    credential = expectations.get("controlled_spy_credential")
    require(type(credential) is dict and set(credential) == {"value", "sha256"} and
            type(credential["value"]) is str and credential["value"] and
            runtime_inputs["spy_credential"] == credential["value"] and
            sha(credential["value"].encode()) == credential["sha256"],
            "original independent controlled credential required")
    rules = spy_recipe.get("rules") if type(spy_recipe) is dict else None
    require(type(rules) is list and rules, "original controlled spy rules required")
    if "spy_credentials" in runtime_inputs or "controlled_spy_credentials" in expectations:
        authorities = expectations.get("controlled_spy_credentials")
        require(type(authorities) is dict and set(authorities) == set(SPACES)
                and all(type(row) is dict and set(row) == {"credential_ref", "value", "sha256"}
                        and row["value"] == "synthetic-controlled-provider-key-" + space
                        and row["credential_ref"] == "FULL_WORKLOAD_SPY_KEY_" + space.upper().replace("-", "_")
                        and sha(row["value"].encode()) == row["sha256"] for space, row in authorities.items()),
                "both independent per-space controlled credentials required")
        selected = {space: {key: row[key] for key in ("credential_ref", "value")}
                    for space, row in authorities.items()}
        require(len({row["value"] for row in selected.values()}) == 2
                and len({row["credential_ref"] for row in selected.values()}) == 2
                and encode(runtime_inputs.get("spy_credentials")) == encode(selected),
                "runtime per-space controlled credentials differ")
        require(all(type(rule) is dict and rule.get("space") in authorities
                    and rule.get("credential_sha256") == authorities[rule["space"]]["sha256"] for rule in rules),
                "per-space controlled credential differs from original spy rules")
    else:
        require(all(type(rule) is dict and rule.get("credential_sha256") == credential["sha256"] for rule in rules),
                "controlled credential differs from original spy rules")
    embedding_keys = [(rule["credential_sha256"], rule["model"], rule["path"])
                      for rule in rules if rule.get("path") == "/v1/embeddings"]
    require(len(embedding_keys) == len(set(embedding_keys)), "ambiguous controlled embedding rule authority")
    authorities = {}
    for rule in rules:
        if rule.get("path") in ("/v1/embeddings", "/v1/responses", "/v1/messages"):
            key = (rule["credential_sha256"], rule["model"], rule["path"])
            authorities.setdefault(key, set()).add(rule["space"])
    require(all(len(spaces) == 1 for spaces in authorities.values()), "ambiguous controlled provider space authority")
    requests = configuration_requests(expectations, {space: runtime_inputs["spy_endpoint"] for space in SPACES})
    if "spy_credentials" in runtime_inputs:
        for request in requests.values():
            connections = request["processing"]["connections"]
            require(connections and all(row["credential_ref"] == selected[request["name"]]["credential_ref"]
                                        for row in connections.values()),
                    "per-space connection differs from controlled credential")
    templates = runtime_principal_expectations(expectations)
    fw10 = expectations["cases"]["FW10"]["spaces"]
    require(len(fw10) == 2 and {case["space"] for case in fw10} == set(SPACES),
            "both original revocable-principal cases required")
    principals = {}
    subjects = set()
    for space in SPACES:
        expected = expectations["configurations"][space]
        grant = expected["grant"]
        require(grant["active"] is True and grant["space_id"] == space and grant["subject"] == expected["subject"],
                "original active data grant expectation differs")
        require(type(templates[space]) is dict and set(templates[space]) == {"manage", "revocable"},
                "explicit original management and revocable principals required")
        principals[space] = {"data": {"subject": expected["subject"], "scopes": grant["scopes"]}, **templates[space]}
        case = next(case for case in fw10 if case["space"] == space)
        require(case["role"] == "revocable" and case["subject"] == principals[space]["revocable"]["subject"],
                "runtime revocable principal differs from original FW10")
        for principal in principals[space].values():
            require(type(principal) is dict and set(principal) == {"subject", "scopes"} and
                    type(principal["subject"]) is str and principal["subject"] and
                    principal["subject"] not in subjects and type(principal["scopes"]) is list and
                    principal["scopes"] and all(type(scope) is str and scope for scope in principal["scopes"]) and
                    len(principal["scopes"]) == len(set(principal["scopes"])),
                    "distinct explicit original principal and scopes required")
            subjects.add(principal["subject"])
    document = decode(encode({"format": "recall-full-workload-runtime-plan/v1", **runtime_inputs,
                             "spaces": [requests[space] for space in SPACES], "principals": principals,
                             "worker_runtime_expectations": worker_runtime_expectations(expectations)}))
    require(len(encode(document)) <= FILE_LIMIT, "original runtime document exceeds input bound")
    return document, sha(encode(document))


def driver_input_document(plan, plan_sha256, provenance):
    """Return a bounded file-reference document for the existing CLI reader.

    The caller writes and pins this source document under its own original
    allocation. The CLI checks every original file again before execution.
    This helper writes nothing and does not allocate or authorize a run.
    """
    require(sha(encode(plan)) == plan_sha256 == provenance["plan_sha256"],
            "resolved original plan changed before export")
    recipes = provenance["graph_recipe_files"]
    require(set(recipes) == set(plan["graph_recipes"]), "original recipe file inventory differs")
    references = {}
    file_bytes = 0
    for key, original in recipes.items():
        require(type(original) is dict and set(original) == {"path", "sha256", "bytes", "canonical_sha256"},
                "original recipe file custody required")
        path = PurePosixPath(original["path"])
        require(path.is_absolute() and ".." not in path.parts and str(path) == original["path"],
                "canonical original recipe path required")
        require(type(original["bytes"]) is int and 0 < original["bytes"] <= FILE_LIMIT and
                type(original["sha256"]) is str and len(original["sha256"]) == 64 and
                all(char in "0123456789abcdef" for char in original["sha256"]),
                "original bounded recipe file hash required")
        require(sha(encode(plan["graph_recipes"][key])) == original["canonical_sha256"],
                "resolved recipe differs from its original file")
        references[key] = {"file": {"path": original["path"], "sha256": original["sha256"]}}
        file_bytes += original["bytes"]
    document = {**plan, "graph_recipes": references}
    raw = encode(document)
    require(len(raw) <= FILE_LIMIT and len(raw) + file_bytes <= PACKAGE_LIMIT,
            "externalized case input exceeds original CLI bounds")
    # Return detached values; a caller cannot modify the original plan through
    # this source-document result. Actual CLI allocation inputs remain separate.
    return decode(raw), {"document_sha256": sha(raw), "resolved_plan_sha256": plan_sha256,
                         "document_bytes": len(raw), "case_input_bytes": len(raw) + file_bytes}


def assemble(bundle, roots, expectation_descriptor):
    """Return (driver_plan, canonical_sha256, source_provenance).

    ``expectation_descriptor`` is an externally reviewed D file/hash, not
    candidate output. Missing independent literals are errors, never defaults.
    This does not supply an allocation, authorize execution, or create receipts.
    """
    require(bundle.get("format") == "recall-full-workload-plan-package/v1" and
            bundle.get("execution_authorized") is False and
            bundle.get("provider_dispatch_authorized") is False and
            bundle.get("product_acceptance") is False, "source-only package required")
    inputs = Inputs(roots)
    contract = inputs.read(bundle["inputs"]["contract"])
    corpus = inputs.read(bundle["inputs"]["corpus"])
    rendered = inputs.read(bundle["inputs"]["rendered"])
    manifest = inputs.read(bundle["inputs"]["graph_manifest"])
    preparation = inputs.read(bundle["inputs"]["preparation"])
    for descriptor in bundle["components"].values():
        inputs.read(descriptor, json_value=False)
    require(contract["controlled_fixture_limits"] == bundle["controlled_fixture_limits"],
            "original workload allocation limits differ")
    require([row["id"] for row in contract["phase_inventory"]] == list(CASES),
            "original fourteen-case contract differs")
    require(manifest["frozen_contracts"]["full-workload-contract.json"] ==
            bundle["inputs"]["contract"]["sha256"] and
            manifest["frozen_contracts"]["workload-corpus.json"] == bundle["inputs"]["corpus"]["sha256"]
            and manifest["frozen_contracts"]["workload-preparation-contract.json"] ==
            bundle["inputs"]["preparation"]["sha256"], "Graph package refers to different original inputs")
    # E names the two large Graph repeats FW03_graph_capacity; L executes
    # those same reviewed identities immediately before FW12's lexical load.
    allowed_recipe_cases = set(CASES) | {"FW03_graph_capacity"}
    entries = {row["id"]: row for row in manifest["recipes"]
               if any(case in allowed_recipe_cases for case in row["cases"])}
    require(set(entries) == set(bundle["graph_recipe_ids"]) and len(entries) == 24,
            "required original Graph recipe inventory differs")
    recipes = {key: inputs.read(recipe_descriptor(bundle, entries[key])) for key in bundle["graph_recipe_ids"]}
    spy_entry = next((row for row in manifest["spy_recipes"] if row["path"] == "workload-spy.json"), None)
    require(spy_entry is not None, "original workload spy input missing")
    spy = inputs.read(recipe_descriptor(bundle, spy_entry))
    require(sum(rule["maximum_calls"] for rule in spy["rules"]) <=
            contract["controlled_fixture_limits"]["owned_spy_dispatches_per_backend"], "spy cap expanded")
    validate_population(contract, corpus, rendered, recipes)
    expected = inputs.read(expectation_descriptor)
    require(expected.get("format") == "recall-full-workload-plan-expectations/v1" and
            expected.get("contract_sha256") == bundle["inputs"]["contract"]["sha256"] and
            expected.get("graph_manifest_sha256") == bundle["inputs"]["graph_manifest"]["sha256"],
            "independent expectations refer to different inputs")
    require(set(expected["configurations"]) == set(SPACES) and set(expected["cases"]) == set(CASES),
            "both configurations and all fourteen independent cases required")
    worker_expectations = worker_runtime_expectations(expected)
    principal_expectations = runtime_principal_expectations(expected)
    for space in SPACES:
        configuration = expected["configurations"][space]
        require({"configuration", "subject", "grant"} <= set(configuration) and
                configuration["configuration"]["security"] == preparation["security_by_space"][space],
                "original policy3/7 configuration and grant required")
    validate_load(expected["load_plan"])
    validate_cases(expected["cases"], recipes)
    load_case = expected["cases"]["FW12"]
    validate_load_evidence(expected["load_plan"], load_case["query_requirements"],
                           load_case.get("query_requirements_sha256"))
    large = {}
    for key in bundle["large_graph_recipe_ids"]:
        recipe = recipes[key]
        source = recipe["source_request"]["source"]
        require(source["external_id"] == "full/large" and len(recipe["source_request"]["doc"]["text"].encode()) == 2_000_000
                and source["hash"] == contract["full_fixture"]["large_recipe"]["sha256"],
                "exact original large input differs")
        large[source["space_id"]] = {"rendered_text": recipe["rendered_text"], "rendered_sha256": recipe["rendered_sha256"]}
    require(set(large) == set(SPACES), "both independently expected large renderings required")
    plan = {"format": "recall-full-workload-case-plan/v1", "contract_sha256": bundle["inputs"]["contract"]["sha256"],
            "configurations": expected["configurations"], "worker_runtime_expectations": worker_expectations,
            "runtime_principal_templates": principal_expectations,
            "baseline_rendered": rendered["sources"],
            "large_rendered": large, "graph_recipes": recipes,
            "graph_recipe_sha256": {key: entries[key]["canonical_sha256"] for key in recipes},
            "spy_recipe": spy, "spy_recipe_sha256": spy_entry["canonical_sha256"],
            "load_plan": expected["load_plan"], "load_plan_sha256": sha(encode(expected["load_plan"])),
            "cases": expected["cases"]}
    observed_files = {(row["root"], row["path"]): row for row in inputs.observed}
    recipe_files = {}
    for key, entry in entries.items():
        descriptor = recipe_descriptor(bundle, entry)
        original = observed_files[(descriptor["root"], descriptor["path"])]
        recipe_files[key] = {"path": original["resolved_path"], "sha256": original["sha256"],
                             "bytes": original["bytes"], "canonical_sha256": entry["canonical_sha256"]}
    provenance = {"format": "recall-full-workload-plan-assembly/v1", "inputs": inputs.observed,
                  "graph_recipe_files": recipe_files,
                  "package_canonical_sha256": sha(encode(bundle)),
                  "expectations_sha256": expectation_descriptor["sha256"], "plan_sha256": sha(encode(plan)),
                  "source_bytes_read": inputs.bytes, "execution_authorized": False,
                  "provider_dispatch_authorized": False, "product_acceptance": False,
                  "required_runtime_inputs": bundle["required_runtime_inputs"]}
    return plan, provenance["plan_sha256"], provenance
