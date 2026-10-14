#!/usr/bin/env python3
"""Start reviewed packaged services inside the actual approved gateway namespace.

Inert without --execute-authorized. Startup observations remain unqualified.
There is no host fallback, build, package installation, credential discovery,
synthetic model reply, or service adoption. The original gateway clock is shared.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import importlib.util
import os
from pathlib import Path
import re
import secrets
import sys
import time

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parent))
from runtime_package_bootstrap_contract import (
    BOOTSTRAP_SCOPES, CHECKS, D28_ALLOCATION_SHA256, FORMAT, IDENTITIES, LIMITS, SCOPE, denied, encode,
    SEMANTIC140_ALLOCATION_SHA256, SEMANTIC140_PROFILE_ID, SEMANTIC140_ANCHOR_FORMAT,
    fields, require, sha, strict_json, token_lifetime, utc, validate_config, validate_plan, validate_receipt,
    worker_recipe_observation, embedding_recipe_observation,
)
from runtime_package_bootstrap_io import (
    Allocation, DEPENDENCIES, Processes, PublicCapture, ROOT, ensure_ports_absent,
    file_hash, load_helpers, private_new, read_bound, read_reference, verify_isolation,
    reviewed_module,
)
from runtime_package_bootstrap_custody import load_original_inputs, write_runtime_custody
from runtime_package_bootstrap_evidence import capture_live_startup

BINARY = Path("/opt/recall/bin/elephant")
PAIRED = Path("/opt/recall/bin/recall-surreal")


BOOTSTRAP_STAGE = "entry"
FAILURE_PATH = Path("/work/bootstrap.failure.json")


def mark_stage(stage):
    global BOOTSTRAP_STAGE
    BOOTSTRAP_STAGE = stage


def failure_diagnostic(error):
    # This is the packaged reviewed bridge, imported only while reporting a
    # failure. It cannot authorize work, execute run(), or change acceptance.
    path = ROOT / "tests/recall_preview_semantics/gateway/bridge.py"
    spec = importlib.util.spec_from_file_location("bootstrap_failure_writer", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.failure_diagnostic(error, FAILURE_PATH, stage=BOOTSTRAP_STAGE, source_root=ROOT)


def inventory():
    return {"format": "recall-runtime-bootstrap-inventory/v1", "qualification_scope": SCOPE,
            "qualified": False, "product_acceptance": False, "execution_authorized": False,
            "limits": LIMITS, "dependency_paths": sorted(DEPENDENCIES),
            "gateway_context_authority": "actual reviewed gateway bridge only",
            "ports": {"gateway": 43129, "recall": 43130, "paired": 43131}}


class Bootstrap:
    def __init__(self, plan, plan_raw, context, context_raw, allocation, wire, gateway):
        self.plan, self.plan_raw, self.context, self.context_raw = plan, plan_raw, context, context_raw
        self.allocation, self.wire, self.gateway = allocation, wire, gateway
        self.started_ns = time.monotonic_ns()
        self.root = Path(plan["run_directory"])
        require(self.root.resolve() == self.root and not self.root.exists() and not self.root.is_symlink(),
                "startup directory must be new within the private gateway work tree")
        self.root.mkdir(mode=0o700)
        for name in ("private", "captures", "logs", "service", "paired", "media", "recovery", "backups"):
            (self.root / name).mkdir(mode=0o700)
        self.http = PublicCapture(wire, self.root, allocation)
        require(hasattr(wire.helpers, "process_capture"), "original process capture helper is unavailable")
        self.processes = Processes(wire.helpers, self.root, allocation,
            capture_module=wire.helpers.process_capture, reserve_capture=self.http.reserve_capture)
        self.checks = {name: [] for name in CHECKS}
        self.configurations, self.token_files = {}, {}
        self.recall = self.paired = None
        self.schema_operation = None
        self.worker_recipe_observation = None
        self.embedding_recipe_observations = {}
        self.env = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "RUST_LOG": "warn",
                    "HOME": str(self.root / "private"), "ELEPHANT_HOST": "127.0.0.1", "ELEPHANT_PORT": "43130",
                    "JWT_ALGORITHM": "HS256", "JWT_ISSUER": "recall-primary-bootstrap", "JWT_AUDIENCE": "recall-primary",
                    "ELEPHANT_DURABILITY_PATH": str(self.root / "recovery"),
                    "ELEPHANT_SNAPSHOT_DIR": str(self.root / "backups"),
                    "PIPELINE_SKILLS_DIR": "/opt/recall/share/native/skills"}
        secret = os.environ.get("JWT_SECRET", "")
        require(re.fullmatch(r"[A-Za-z0-9_-]{64}", secret) and os.environ.get("JWT_ALGORITHM") == "HS256"
                and "SKIP_AUTH" not in os.environ and "JWT_SECRET_FILE" not in os.environ,
                "only actual bridge-created synthetic API signing material is supported")
        self.key = self.root / "private/signing.key"
        private_new(self.key, secret.encode())
        self.env["JWT_SECRET_FILE"] = str(self.key)
        self.gateway_names = set()
        for space in plan["spaces"]:
            for connection in space["processing"]["connections"].values():
                name = connection["credential_ref"]
                require(type(name) is str and re.fullmatch(r"RECALL_GATEWAY_[a-zA-Z0-9_]{1,120}", name),
                        "provider credential reference must name an actual gateway capability")
                require(connection["endpoint"] == "http://127.0.0.1:43129/v1"
                        and name in os.environ and 16 <= len(os.environ[name]) <= 256,
                        "actual isolated gateway binding is unavailable")
                self.gateway_names.add(name)
                self.env[name] = os.environ[name]
        # Persist no secret values in plans, captures or process metadata.
        private_new(self.root / "plan.json", plan_raw)
        private_new(self.root / "launch-context.json", context_raw)

    def record(self, check, response):
        require(check in self.checks, "unreviewed startup check")
        self.checks[check].append(response["request_index"])

    async def issue(self, role, subject, *, space=None, bootstrap=False, read_only=False):
        token_path = self.root / "private" / (role + ".token")
        seconds = token_lifetime(min(
            (min(self.allocation.deadline_ns, self.allocation.whole_deadline_ns) - time.monotonic_ns()) / 10**9,
            (self.allocation.expiry - dt.datetime.now(dt.timezone.utc)).total_seconds()),
            management=space is None or bootstrap)
        arguments = ["service-token", "issue", "--secret-file", str(self.key), "--subject", subject,
                     "--expires-in", str(seconds), "--issuer", self.env["JWT_ISSUER"],
                     "--audience", self.env["JWT_AUDIENCE"], "--output", str(token_path)]
        if space is None:
            arguments += ["--global-admin"]
            scopes = ["read:status", "read:system", "write:system"]
        else:
            clearance = [{"policy_id": space["security"]["policy_id"], "max_level": space["security"]["level"],
                          "labels": [], "handling_allow": []}]
            clearance_path = self.root / "private" / (role + ".clearances.json")
            private_new(clearance_path, encode(clearance))
            arguments += ["--bootstrap-space" if bootstrap else "--space", space["name"],
                          "--clearances-file", str(clearance_path)]
            scopes = list(BOOTSTRAP_SCOPES) if bootstrap else self.plan["service_scopes"]
            if read_only:
                scopes = [scope for scope in scopes if scope.startswith("read:")]
                arguments += ["--read-only"]
        for scope in scopes:
            arguments += ["--scope", scope]
        child = self.processes.launch(BINARY, arguments, self.env, purpose="issue-" + role, cwd=self.root / "service")
        await self.processes.wait(child, 15)
        token = read_bound(token_path, 16384, private=True).decode("ascii").strip()
        require(re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", token), "CLI issued no signed token")
        self.http.tokens[role] = token
        return token_path

    async def credentials(self):
        await self.issue("operator", "primary-runtime-operator")
        for space in self.plan["spaces"]:
            name = space["name"]
            await self.issue("bootstrap-" + name, "primary-bootstrap-" + name, space=space, bootstrap=True)
            self.token_files[name] = str(await self.issue("service-" + name, "primary-service-" + name, space=space))
            await self.issue("read-only-" + name, "primary-read-only-" + name, space=space, read_only=True)

    async def wait_http(self, path, child, *, port=43130, role=None, ready=False, maximum=30):
        deadline = time.monotonic() + min(maximum, self.allocation.remaining())
        while time.monotonic() < deadline:
            require(child.poll() is None, "owned service exited during bounded startup")
            response = await self.http.request("GET", path, port=port, role=role)
            if response["error_type"] is None and response["framing_complete"] and response["status"] == 200:
                if not ready:
                    return response
                value = response["body"]
                if (value.get("ready") is True and value.get("configured") is True
                        and value.get("storage_ready") is True and value.get("recovery_ready") is True
                        and value.get("draining") is False):
                    return response
            # Polling only observes readiness. No mutation or original work is retried.
            await asyncio.sleep(min(0.25, self.allocation.remaining()))
        raise ValueError("owned service did not become ready within original startup allocation")

    async def services(self):
        if self.plan["backend"] == "surreal-db":
            password = "synthetic-" + secrets.token_urlsafe(32)
            database_env = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "RUST_LOG": "warn",
                            "HOME": str(self.root / "private"), "SURREAL_USER": "qualification", "SURREAL_PASS": password}
            self.paired = self.processes.launch(PAIRED,
                ["start", "--bind", "127.0.0.1:43131", "file:" + str(self.root / "paired/database.db")],
                database_env, purpose="paired-persistent-server", cwd=self.root / "paired")
            await self.wait_http("/health", self.paired, port=43131)
            self.env.update(ELEPHANT_DB="surreal-db", SURREALDB_URL="ws://127.0.0.1:43131",
                            SURREALDB_NAMESPACE="primary", SURREALDB_DATABASE="fixture",
                            SURREALDB_USER="qualification", SURREALDB_PASS=password)
            child = self.processes.launch(BINARY, ["migrate"], self.env, purpose="explicit-fresh-schema", cwd=self.root / "service")
            process_index = await self.processes.wait(child, 90)
            self.schema_operation = {"kind": "migrate", "capture_index": None, "process_index": process_index}
        self.recall = self.processes.launch(BINARY, ["serve", "--server-only", "--shutdown-timeout-seconds", "60"],
            self.env, purpose="recall-authenticated-service", cwd=self.root / "service")
        await self.wait_http("/v1/health", self.recall)
        denied_response = await self.http.request("GET", "/v1/readiness")
        denied(denied_response, 401, "unauthorized")
        self.record("unauthenticated_readiness", denied_response)
        # Runtime control is authenticated global management, never a data token.
        response = await self.http.request("POST", "/mcp/full", role="operator", mcp=True, body={
            "jsonrpc": "2.0", "id": self.http.count + 1, "method": "initialize", "params": {
                "protocolVersion": "2025-03-26", "capabilities": {},
                "clientInfo": {"name": "runtime-bootstrap", "version": "1"}}})
        self.http.success(response)
        patch = {"ingest_enabled": False, "extractor_required": False,
                 "multimodal_asset_root": str(self.root / "media")}
        if self.plan["backend"] == "elephant-db":
            unconfigured = await self.http.request("GET", "/v1/readiness", role="operator")
            require(unconfigured["status"] == 503 and unconfigured["body"]["configured"] is False,
                    "fresh embedded serve unexpectedly initialized schema")
            value, capture_index = await self.http.tool("initialize_runtime", {
                **patch, "backend": "elephant-db", "snapshot_path": str(self.root / "service/state.snapshot")})
            self.schema_operation = {"kind": "initialize_runtime", "capture_index": capture_index, "process_index": None}
        else:
            value, _ = await self.http.tool("configure_runtime", patch)
        require(value.get("configured") is True and value.get("storage_ready") is True
                and value.get("backend") == self.plan["backend"], "explicit storage initialization failed")
        await self.wait_http("/v1/readiness", self.recall, role="operator", ready=True)

    async def grant(self, name, role, scopes, *, active=True, generation=None):
        subject = "primary-" + role + "-" + name
        path = f"/v1/spaces/{name}/grants/{subject}"
        response = await self.http.request("PUT", path, role="bootstrap-" + name, space=name,
            body={"active": active, "scopes": scopes, "expected_generation": generation})
        value = self.http.success(response)
        require(value.get("space_id") == name and value.get("subject") == subject
                and value.get("active") is active and set(value.get("scopes", [])) == set(scopes)
                and type(value.get("generation")) is str and value["generation"], "actual stored service grant differs")
        readback = await self.http.request("GET", path, role="bootstrap-" + name, space=name)
        require(self.http.success(readback) == value, "current grant readback differs")
        self.record("grant_readback", readback)
        return value, response

    async def empty_inventory(self, name, check):
        response = await self.http.request("GET", f"/v1/spaces/{name}/sources?limit=100", role="service-" + name, space=name)
        value = self.http.success(response)
        require(value.get("sources") == [] and value.get("next_cursor") is None,
                "fresh persistent space already contains sources")
        self.record(check, response)

    async def observe_worker_recipe(self):
        value, index = await self.http.tool("get_runtime_config", {})
        observed = worker_recipe_observation(value, self.plan["backend"], index,
                                             self.worker_recipe_observation)
        self.worker_recipe_observation = observed
        return observed

    async def observe_embedding_recipes(self):
        """Observe current approved per-space factories; never resolve secrets here."""
        observed = {}
        for space in self.plan["spaces"]:
            name = space["name"]
            response = await self.http.request("GET", f"/v1/spaces/{name}/status",
                                               role="service-" + name, space=name)
            value = self.http.success(response)
            config = self.configurations[name]
            observed[name] = embedding_recipe_observation(name, value, config, response["request_index"])
        if self.embedding_recipe_observations:
            require(set(observed) == set(self.embedding_recipe_observations)
                    and all(value["recipes_sha256"] == self.embedding_recipe_observations[name]["recipes_sha256"]
                            for name, value in observed.items()), "original embedding factory recipe changed")
        # Commit only after every original named space has matched.
        self.embedding_recipe_observations = observed
        return observed

    async def spaces(self):
        for space in self.plan["spaces"]:
            name = space["name"]
            response = await self.http.request("POST", "/v1/spaces", role="bootstrap-" + name, space=name, body=space)
            created = validate_config(space, self.http.success(response, 201), "primary-bootstrap-" + name)
            readback = await self.http.request("GET", "/v1/spaces/" + name, role="bootstrap-" + name, space=name)
            require(self.http.success(readback) == created, "bootstrap actual configuration readback differs")
            self.record("named_config_readback", readback)
            self.configurations[name] = created
            missing = await self.http.request("GET", f"/v1/spaces/{name}/sources?limit=100", role="service-" + name, space=name)
            denied(missing, 403, "forbidden_space")
            self.record("missing_grant", missing)
            grant, _ = await self.grant(name, "service", self.plan["service_scopes"])
            read_scopes = [scope for scope in self.plan["service_scopes"] if scope.startswith("read:")]
            await self.grant(name, "read-only", read_scopes)
            ordinary = await self.http.request("GET", "/v1/spaces/" + name, role="service-" + name, space=name)
            require(self.http.success(ordinary) == created, "ordinary actual configuration readback differs")
            self.record("ordinary_config_readback", ordinary)
            await self.empty_inventory(name, "ordinary_empty_inventory")
            bootstrap_denial = await self.http.request("GET", f"/v1/spaces/{name}/sources?limit=100",
                                                        role="bootstrap-" + name, space=name)
            denied(bootstrap_denial, 403, "forbidden_space")
            self.record("bootstrap_data_denial", bootstrap_denial)
            read_denial = await self.http.request("PUT", f"/v1/spaces/{name}/sources/bootstrap-denied",
                role="read-only-" + name, space=name, body={"revision": "denied-v1", "text": "denial control",
                    "media_type": "text/plain", "source_url": None, "observed_at": None, "labels": {}, "expected_generation": None})
            denied(read_denial, 403, "forbidden_space")
            self.record("read_only_write_denial", read_denial)
            revoked, _ = await self.grant(name, "service", self.plan["service_scopes"], active=False, generation=grant["generation"])
            denial = await self.http.request("GET", f"/v1/spaces/{name}/sources?limit=100", role="service-" + name, space=name)
            denied(denial, 403, "forbidden_space")
            self.record("grant_revocation", denial)
            await self.grant(name, "service", self.plan["service_scopes"], generation=revoked["generation"])
            restored = await self.http.request("GET", "/v1/spaces/" + name, role="service-" + name, space=name)
            require(self.http.success(restored) == created, "grant restoration did not restore original ordinary read")
            self.record("grant_restoration", restored)
        first, second = [space["name"] for space in self.plan["spaces"]]
        for own, other in ((first, second), (second, first)):
            response = await self.http.request("GET", "/v1/spaces/" + other, role="service-" + own, space=other)
            denied(response, 403, "forbidden_space")
            self.record("wrong_space", response)
        # Workers start only after the listener, explicit schema and empty-space
        # checks. No source, search, model probe, reprocess or wiki is submitted.
        await self.http.tool("configure_runtime", {"ingest_enabled": True, "extractor_required": False})
        ready = await self.wait_http("/v1/readiness", self.recall, role="operator", ready=True)
        require(ready["body"].get("worker_ready") is True and ready["body"].get("semantic_ingest_ready") is True,
                "actual managed workers are not ready")
        self.record("ready", ready)
        await self.observe_worker_recipe()
        await self.observe_embedding_recipes()
        for space in self.plan["spaces"]:
            await self.empty_inventory(space["name"], "final_empty_inventory")

    def receipt(self, isolation_raw):
        processes = {"recall": self.processes.identity(self.recall, str(BINARY), self.plan["artifact_sha256"])}
        if self.paired:
            processes["paired"] = self.processes.identity(self.paired, str(PAIRED), self.plan["paired_artifact_sha256"])
        receipt = {"format": FORMAT, "status": "bootstrapped", "qualification_scope": SCOPE,
            "qualified": False, "product_acceptance": False, **{key: self.plan[key] for key in set(IDENTITIES) - {"configuration_sha256"}},
            "configuration_sha256": self.context["anchor"]["configuration_sha256"],
            "authentication_enabled": True, "clean_persistent_store": True, "schema_explicitly_initialized": True,
            "schema_operation": self.schema_operation, "launch_context_sha256": sha(self.context_raw),
            "isolation_sha256": sha(isolation_raw), "plan_sha256": sha(self.plan_raw),
            "allocation_started_monotonic_ns": self.allocation.started_ns, "started_monotonic_ns": self.started_ns,
            "observed_monotonic_ns": time.monotonic_ns(), "original_deadline_monotonic_ns": self.allocation.deadline_ns,
            "whole_deadline_monotonic_ns": self.allocation.whole_deadline_ns,
            "space_requests_sha256": sha(encode(self.plan["spaces"])),
            "observed_configurations_sha256": sha(encode(self.configurations)),
            "processes": processes, "bootstrap_process": self.gateway.process_identity(),
            "process_commands": self.processes.command_receipt(), "checks": self.checks, "captures": self.http.captures,
            "configurations": self.configurations, "token_files": self.token_files,
            "runtime_config_sha256": file_hash(self.root / "service/.elephant/elephant_config.toml")}
        return validate_receipt(receipt, self.plan, maximum_backend_seconds=self.allocation.maximum_seconds)


def static_input(path):
    path = Path(path)
    require(path.is_absolute() and path.is_relative_to("/inputs") and path.resolve() == path,
            "original static inputs must be in the read-only /inputs mount")
    return read_bound(path, 8 * 1024**2, private=True)


def phase_inputs(plan_path, review_path, template_path):
    mark_stage("phase_inputs")
    plan_raw, review_raw, template_raw = [static_input(path) for path in (plan_path, review_path, template_path)]
    plan, review, template = [strict_json(raw) for raw in (plan_raw, review_raw, template_raw)]
    validate_plan(plan)
    require(review.get("format") in ("recall-primary-driver-review/v1", "recall-primary-driver-review/v2")
            and review.get("approved") is True
            and review.get("reviewer") == "qualification (independent D lane)", "independent immutable input review absent")
    require(review["primary_template_sha256"] == sha(template_raw)
            and review["inputs"]["bootstrap_plan"] == sha(plan_raw)
            and review["dependencies"] == plan["dependencies"], "actual immutable phase was not independently reviewed")
    required = {"format", "execution_authorized", "provider_dispatch_authorized", "authentication_enabled",
                "source_commit", "artifact_sha256", "paired_artifact_sha256", "runtime_sha256",
                "backend", "endpoint", "nonce", "expires_at", "fixture_timestamp", "inputs"}
    fields(template, required)
    version = 2 if template["format"] == "recall-primary-provider-run/v2" else 1
    require(template["format"] == f"recall-primary-provider-run/v{version}"
            and review["format"] == f"recall-primary-driver-review/v{version}"
            and template["execution_authorized"] is True and template["provider_dispatch_authorized"] is True
            and template["authentication_enabled"] is True, "original phase permission is absent")
    for key in set(IDENTITIES) - {"configuration_sha256"}:
        require(plan[key] == template["nonce" if key == "run_nonce" else key], "reviewed primary template identity differs")
    require(template["expires_at"] == plan["expires_at"] and dt.datetime.now(dt.timezone.utc) < utc(plan["expires_at"]),
            "original phase approval expired or changed")
    require(review["nonce"] == plan["run_nonce"] and review["backend"] == plan["backend"]
            and review["endpoint"] == plan["endpoint"], "review belongs to another original phase")
    names = {"corpus", "expectations", "proposal", "configurations", "conditional_cases", "bootstrap_plan", "rendered_map"}
    if version == 2:
        names |= {"runtime_review", "runtime_original_inputs"}
    fields(template["inputs"], names)
    require(template["inputs"]["bootstrap_plan"] == {"path": str(plan_path), "sha256": sha(plan_raw)},
            "primary template changed its original bootstrap plan")
    conditional = None
    for name, reference in template["inputs"].items():
        fields(reference, {"path", "sha256"})
        raw = static_input(reference["path"])
        require(sha(raw) == reference["sha256"] == review["inputs"][name], "immutable reviewed input differs: " + name)
        if name == "conditional_cases" and version == 2:
            conditional = strict_json(raw)
    if version == 2:
        require(type(conditional) is dict and conditional.get("format") == "recall-primary-conditional-cases/v1"
                and conditional.get("reviewer") == "qualification (independent D lane)"
                and conditional.get("status") == "approved-before-execution"
                and type(conditional.get("cases")) is list
                and all(type(case) is dict for case in conditional["cases"]),
                "original conditional scope review is absent")
        scoped_cases = any(case.get("category") == "wiki" and case.get("tool") == "compile_wiki_article"
                           for case in conditional["cases"])
        require((plan.get("scoped_wiki") is True) == scoped_cases,
                "scoped wiki plan and reviewed cases differ")
    else:
        require("scoped_wiki" not in plan, "scoped wiki requires the current reviewed primary phase")
    phase = {"plan": plan, "plan_raw": plan_raw, "review": review, "review_raw": review_raw,
             "template": template, "review_reference": {"path": str(review_path), "sha256": sha(review_raw)}}
    if version == 2:
        mark_stage("runtime_original_inputs")
        phase["runtime_originals"] = load_original_inputs(template["inputs"]["runtime_original_inputs"],
                                                           template["inputs"]["runtime_review"])
    return phase


def launch_expectations(gateway, expected, allocation_sha256):
    """Keep static review inputs acyclic and forward their exact expectations."""
    require(allocation_sha256 in (None, D28_ALLOCATION_SHA256, SEMANTIC140_ALLOCATION_SHA256), "unreviewed gateway allocation")
    required = (gateway.HASH_FIELDS - {"manifest_sha256", "configuration_sha256"}) | {
        "source_commit", "run_nonce", "allocation_sha256"}
    if allocation_sha256 in (D28_ALLOCATION_SHA256, SEMANTIC140_ALLOCATION_SHA256):
        required |= {"format", "caps"}
    if allocation_sha256 == SEMANTIC140_ALLOCATION_SHA256:
        required.add("profile_id")
    require(type(expected) is dict and set(expected) == required
            and expected["allocation_sha256"] == allocation_sha256,
            "complete original launch allocation expectations required")
    if allocation_sha256 == D28_ALLOCATION_SHA256:
        require(expected["format"] == gateway.ANCHOR_FORMAT and encode(expected["caps"]) == encode({
            "physical_dispatches": 2100, "maximum_nano_usd": 3_000_000_000}),
            "original D28 anchor format or caps differ")
    if allocation_sha256 == SEMANTIC140_ALLOCATION_SHA256:
        require(expected["format"] == SEMANTIC140_ANCHOR_FORMAT
                and expected["profile_id"] == SEMANTIC140_PROFILE_ID
                and encode(expected["caps"]) == encode({"physical_dispatches": 4260,
                    "maximum_nano_usd": 5_000_000_000}), "closed semantic140 anchor differs")
    return expected


def actual_context(phase, *, allocation_sha256=None):
    mark_stage("actual_context")
    require(allocation_sha256 in (None, D28_ALLOCATION_SHA256, SEMANTIC140_ALLOCATION_SHA256), "unreviewed gateway allocation")
    require(ROOT == Path("/opt/recall"), "actual packaged read-only runtime is required")
    plan, review = phase["plan"], phase["review"]
    path = ROOT / "tests/recall_preview_semantics/gateway/launch_context.py"
    relative = path.relative_to(ROOT).as_posix()
    gateway = reviewed_module("bootstrap_actual_gateway_context", path, plan["dependencies"][relative])
    expected = launch_expectations(gateway, review["expected_launch"], allocation_sha256)
    require(expected["context_helper_sha256"] == plan["dependencies"][relative],
            "mounted and packaged actual gateway validator must be the same reviewed source")
    for key in ("source_commit", "artifact_sha256", "paired_artifact_sha256", "runtime_sha256", "run_nonce"):
        require(expected[key] == plan[key], "launch expectation differs from static plan")
    context_path = os.environ.get("RECALL_GATEWAY_CONTEXT_PATH", "")
    context_sha = os.environ.get("RECALL_GATEWAY_CONTEXT_SHA256", "")
    context = gateway.read_context(context_path, context_sha, expected=expected, immediate_parent=True)
    require(context["anchor"]["input_inventory_sha256"] is not None,
            "actual hash-bound read-only input mount is required")
    raw = read_bound(context_path, 131072, private=True)
    require(sha(raw) == context_sha and strict_json(raw) == context, "actual context changed after validation")
    require(file_hash(BINARY) == plan["artifact_sha256"] and file_hash(PAIRED) == plan["paired_artifact_sha256"],
            "actual packaged binaries differ from original artifacts")
    return gateway, context, raw, {"path": context_path, "sha256": context_sha}


def primary_manifest(phase, receipt, receipt_reference, context_reference, custody_reference=None):
    # The template is independently reviewed before output. This join adds only
    # genuine dynamic observations/paths. Permission flags are never generated.
    template = strict_json(encode(phase["template"]))
    template["inputs"]["review"] = phase["review_reference"]
    template["configuration_sha256"] = receipt["configuration_sha256"]
    template["observations"] = {"runtime_receipt": receipt_reference, "gateway_context": context_reference}
    if template.get("format") == "recall-primary-provider-run/v2":
        require(custody_reference is not None, "v2 requires the actual original runtime custody sidecar")
        template["observations"]["runtime_custody"] = dict(custody_reference)
    else:
        require(custody_reference is None, "historical v1 cannot reinterpret dynamic v2 custody")
    template["token_files"] = receipt["token_files"]
    template["output_directory"] = phase["plan"]["run_directory"] + "/primary-capture"
    return template


async def execute_phase(phase):
    require(phase["template"].get("format") == "recall-primary-provider-run/v2"
            and "runtime_originals" in phase, "historical bootstrap-only v1 is not executable qualification")
    allocation_start = time.monotonic_ns()
    gateway, context, context_raw, context_reference = actual_context(phase)
    plan = phase["plan"]
    mark_stage("load_helpers")
    wire = load_helpers(plan)
    mark_stage("ports_absent")
    ensure_ports_absent()
    whole_deadline = int(context["anchor"]["clock"]["deadline_mono"] * 10**9)
    allocation = Allocation(allocation_start, min(whole_deadline, allocation_start + 2700 * 10**9),
                            whole_deadline, plan["expires_at"])
    allocation.remaining()
    mark_stage("isolation_observation")
    isolation_raw = read_bound(Path("/work/isolation.json"), 131072, private=True)
    verify_isolation(strict_json(isolation_raw))
    instance = None
    error_type, consumer_exit = None, None
    try:
        mark_stage("bootstrap_instance")
        instance = Bootstrap(plan, phase["plan_raw"], context, context_raw, allocation, wire, gateway)
        mark_stage("credentials")
        await instance.credentials()
        mark_stage("services")
        await instance.services()
        mark_stage("spaces")
        await instance.spaces()
        # Revalidate the same actual original bridge/mount/deadline just before
        # handing live services and original captures to the collector.
        gateway.read_context(context_reference["path"], context_reference["sha256"],
                             expected=phase["review"]["expected_launch"], immediate_parent=True)
        mark_stage("startup_custody")
        receipt = instance.receipt(isolation_raw)
        raw = encode(receipt)
        receipt_path = instance.root / "runtime-bootstrap.json"
        private_new(receipt_path, raw)
        receipt_reference = {"path": str(receipt_path), "sha256": sha(raw)}
        custody = capture_live_startup(instance, receipt_reference, context_reference, gateway,
                                       phase["review"]["expected_launch"])
        custody_reference = write_runtime_custody(custody, phase["runtime_originals"], receipt,
                                                 instance.root / "runtime-custody.json")
        gateway.read_context(context_reference["path"], context_reference["sha256"],
                             expected=phase["review"]["expected_launch"], immediate_parent=True)
        allocation.remaining()
        manifest = primary_manifest(phase, receipt, receipt_reference, context_reference, custody_reference)
        manifest_path = instance.root / "primary-run.json"
        private_new(manifest_path, encode(manifest))
        allocation.bootstrap = False  # Changes the local phase ceiling, not either original run deadline.
        child_environment = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "HOME": str(instance.root / "private"),
                             "PYTHONDONTWRITEBYTECODE": "1", "RECALL_GATEWAY_CONTEXT_PATH": context_reference["path"],
                             "RECALL_GATEWAY_CONTEXT_SHA256": context_reference["sha256"]}
        mark_stage("original_collector")
        child = instance.processes.launch(Path("/usr/bin/python3"), ["-I",
            "/opt/recall/scripts/qualification/primary_provider_driver.py", "--manifest", str(manifest_path),
            "--execute-authorized"], child_environment, purpose="original-primary-collector", cwd=instance.root / "private")
        consumer_exit = await instance.processes.wait(child, allocation.remaining())
        return receipt
    except BaseException as error:
        error_type = type(error).__name__
        raise
    finally:
        if instance is not None:
            failures = await instance.processes.cleanup()
            # Separate terminal evidence; never rewrite the original startup
            # receipt, even if the collector or shutdown later fails.
            cleanup = {"format": "recall-runtime-bootstrap-cleanup/v1", "qualified": False,
                       "product_acceptance": False, "backend": plan["backend"], "run_nonce": plan["run_nonce"],
                       "status": "complete" if not failures and error_type is None else "incomplete",
                       "cleanup_complete": not failures,
                       "error_type": error_type, "collector_process_index": consumer_exit,
                       "cleanup_failures": failures, "processes": instance.processes.records,
                       "finished_monotonic_ns": time.monotonic_ns()}
            private_new(instance.root / "cleanup.json", encode(cleanup))
            require(not failures, "owned service cleanup was incomplete")


async def execute(phases):
    require(1 <= len(phases) <= 2 and len({phase["plan"]["backend"] for phase in phases}) == len(phases),
            "one exclusive phase per original backend is allowed")
    common = phases[0]["plan"]
    for phase in phases:
        for key in ("source_commit", "artifact_sha256", "paired_artifact_sha256", "runtime_sha256", "run_nonce"):
            require(phase["plan"][key] == common[key], "backend phases must share the same original runtime/allocation")
    # All immutable files are reviewed before any credentials/processes. Actual
    # context is checked again per phase so a replaced bridge cannot be adopted.
    for phase in phases:
        actual_context(phase)
    for phase in phases:
        await execute_phase(phase)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", nargs=3, action="append", type=Path, metavar=("PLAN", "REVIEW", "PRIMARY_TEMPLATE"))
    parser.add_argument("--execute-authorized", action="store_true")
    args = parser.parse_args()
    if not args.execute_authorized:
        print(encode(inventory()).decode())
        return 0
    try:
        mark_stage("phase_arguments")
        require(args.phase and len(args.phase) <= 2, "explicit original phase inputs required")
        phases = [phase_inputs(*paths) for paths in args.phase]
        asyncio.run(execute(phases))
        return 0
    except BaseException as error:
        try:
            failure_diagnostic(error)
        except Exception:
            pass  # Preserve the original exit even if private diagnostics fail.
        # No exception repr, secret, request body or candidate output on stdout.
        sys.stderr.write("runtime bootstrap or original collection incomplete; inspect private original captures\n")
        return 78


if __name__ == "__main__":
    sys.exit(main())
