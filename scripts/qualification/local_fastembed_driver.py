#!/usr/bin/env python3
"""Finite installed FastEmbed observation; execution needs an external allocation."""
from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
from pathlib import Path
import re
import sys
import time
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


core = load("local_installed_core", HERE / "installed_linux_offline.py")
contract = load("local_fastembed_contract", HERE / "local_fastembed_contract.py")
primary = load("local_primary_contract", HERE / "primary_provider_driver_contract.py")
semantic = load("local_semantic_oracle", ROOT / "tests/recall_preview_semantics/oracle.py")
# controls imports the oracle by its original short name. Restore the registry
# immediately so the installed driver's independent lifecycle oracle is intact.
prior_oracle = sys.modules.get("oracle")
sys.modules["oracle"] = semantic
try:
    controls = load("local_vector_controls", ROOT / "tests/recall_preview_semantics/controls.py")
finally:
    if prior_oracle is None:
        sys.modules.pop("oracle", None)
    else:
        sys.modules["oracle"] = prior_oracle

PLAN = HERE / "local_fastembed_plan.json"
CORPUS = ROOT / "tests/recall_preview_semantics/corpus.json"
RENDERED = ROOT / "tests/recall_preview/primary-rendered-map.json"
APPROVAL = ROOT / "tests/recall_preview/semantic-expectations-approval.json"
require = contract.require


def plan():
    cases = core.read_json(PLAN)
    corpus = core.read_json(CORPUS)
    semantic.validate_corpus(corpus)
    require(primary.digest(corpus) == primary.CORPUS_DIGEST, "original semantic corpus changed")
    approval = core.read_json(APPROVAL)
    require(approval["corpus_canonical_sha256"] == primary.CORPUS_DIGEST
            and approval["status"] == "expectations-approved-before-execution", "original D approval absent")
    require(cases["limits"] == contract.LIMITS and cases["spaces"] == [row["id"] for row in corpus["spaces"]]
            and cases["sources"] == [[s["space"], s["id"], s["revision"]] for s in corpus["sources"]]
            and cases["queries"] == [q["id"] for q in corpus["queries"]], "frozen local fixture changed")
    require(core.file_digest(RENDERED) == primary.RENDERED_MAP_SHA256, "original rendered map changed")
    primary.rendered_contract(core.read_json(RENDERED), corpus, core.file_digest(CORPUS))
    paths = [Path(__file__), PLAN, CORPUS, RENDERED, APPROVAL, core.fixtures.CORPUS_PATH,
             ROOT / "crates/types/src/records/doc_node.rs",
             ROOT / "crates/pipeline/src/scheduler/evidence_work_kinds.rs",
             HERE / "local_fastembed_contract.py", HERE / "primary_provider_driver_contract.py",
             HERE / "test_local_fastembed_driver.py", HERE / "local_fastembed_README.md",
             HERE / "installed_linux_offline.py", Path(semantic.__file__), Path(controls.__file__)]
    hashes = {str(path.relative_to(ROOT)): core.file_digest(path) for path in paths}
    for module in core.LOADED_SOURCE_HASHES:
        hashes[str(Path(module.__file__).relative_to(ROOT))] = core.loaded_digest(module)
    return {"format": "recall-installed-local-plan/v1", "expectations": cases,
            "input_sha256": hashes, "corpus_sha256": primary.CORPUS_DIGEST,
            "execution_authorized": False, "product_acceptance": False}


def original_clock(manifest):
    expiry = core.utc(manifest["expires_at"])
    delta = expiry - dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    wall_ns = (delta.days * 86400 + delta.seconds) * 10**9 + delta.microseconds * 1000
    return core.process_capture.pin_allocation_clock(contract.LIMITS["total_seconds"], wall_ns,
        original_sample=manifest["original_allocation_clock"])


def validate_manifest(path, expected_sha256):
    raw = core.process_capture.read_bounded(path, 1024 * 1024)
    require(core.digest(raw) == expected_sha256, "root allocated manifest bytes differ")
    manifest = core.capture.strict_json_body(raw)
    require(manifest.get("format") == "recall-installed-local-run/v1"
            and manifest.get("execution_authorized") is True and manifest.get("backend") == "surreal-db",
            "actual native local execution not authorized")
    inventory = plan()
    require(manifest["fresh_spaces_authorized"] == inventory["expectations"]["spaces"],
            "both exact fresh named spaces need explicit authority")
    for field, size in (("source_commit", 40), ("archive_sha256", 64), ("binary_sha256", 64),
                        ("expectation_review_sha256", 64), ("runtime_binding_review_sha256", 64)):
        require(isinstance(manifest.get(field), str) and re.fullmatch(f"[a-f0-9]{{{size}}}", manifest[field]),
                "exact trusted identity absent: " + field)
    for field in ("archive", "run_directory", "expectation_review", "runtime_binding_review"):
        value = Path(manifest[field])
        require(value.is_absolute() and value == value.resolve(), "canonical absolute path required: " + field)
    require(re.fullmatch("[a-zA-Z0-9_-]{16,64}", manifest["nonce"]) is not None, "finite fresh nonce required")
    core.utc(manifest["fixture_timestamp"])
    clock, deadline = original_clock(manifest)
    now = time.monotonic_ns()
    require(clock["mono_after_ns"] <= now < deadline - contract.LIMITS["cleanup_seconds"] * 10**9,
            "original allocation has no remaining work window")
    review_raw = core.process_capture.read_bounded(manifest["expectation_review"], 1024 * 1024)
    require(core.digest(review_raw) == manifest["expectation_review_sha256"], "independent review changed")
    review = core.capture.strict_json_body(review_raw)
    require(review.get("format") == "recall-installed-local-review/v1"
            and review.get("reviewer") == "qualification (independent D lane)"
            and review.get("approved") is True and review.get("plan") == inventory,
            "independent exact local plan review absent")
    require(manifest["model_asset_manifest_sha256"] == contract.MODEL_DIGEST
            and manifest["model_asset_root"] == "/model-assets", "original model mount differs")
    require("runtime_library" not in manifest and "runtime_library_sha256" not in manifest
            and "runtime_environment" not in manifest, "shared/ambient runtime override is not approved")
    runtime_raw = core.process_capture.read_bounded(manifest["runtime_binding_review"], 1024 * 1024)
    require(core.digest(runtime_raw) == manifest["runtime_binding_review_sha256"], "original runtime review changed")
    contract.static_runtime(manifest, core.capture.strict_json_body(runtime_raw))
    return manifest, inventory, raw, review_raw


def readonly_mount(path):
    """Require an actual readonly mount, not merely chmod on a writable host tree."""
    path = Path(path).resolve(strict=True)
    candidates = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        fields = line.split()
        mount = Path(re.sub(r"\\([0-7]{3})", lambda item: chr(int(item[1], 8)), fields[4]))
        if path == mount or path.is_relative_to(mount):
            candidates.append((len(mount.parts), fields[5].split(",")))
    require(candidates and "ro" in max(candidates, key=lambda value: value[0])[1],
            "model/input file is not mounted read-only")


class LocalDriver(core.Driver):
    observation_format = "recall-installed-local-observation/v1"

    def __init__(self, manifest, inventory, isolation, raw, review_raw):
        clock, deadline = original_clock(manifest)
        super().__init__(manifest, inventory, isolation, original_manifest_raw=raw)
        # The outer controller starts this clock before installation/namespace
        # setup. Never acquire another work or cleanup allocation at this seam.
        self.allocation_clock, self.allocation_deadline_ns = clock, deadline
        self.started = clock["mono_before_ns"] / 1e9
        self.allocation_deadline = core.process_capture.deadline_seconds(deadline)
        self.work_deadline = self.allocation_deadline - contract.LIMITS["cleanup_seconds"]
        self.review_raw = review_raw
        self.corpus = core.read_json(CORPUS)
        self.rendered = core.read_json(RENDERED)
        self.configurations, self.current = {}, {}
        self.publications, self.query_rows = [], {"http": [], "mcp": []}
        self.polls = 0
        self.cache_before = None
        self.report.update({"local_inference": "unobserved", "semantic_retrieval": "unobserved",
            "installed_fake_vector_control": "unmet: separately allocated installed fake index required",
            "canonical_groundedness": "unqualified: no generation or independent response review",
            "physical_inference_attempts": "unmet: public routes do not expose local attempt identities",
            "harness_retries": 0, "fallback_fetches": 0, "query_rewrites": 0})
        self.environment.update({"OMP_NUM_THREADS": "4", "RAYON_NUM_THREADS": "4", "TOKENIZERS_PARALLELISM": "false",
            "HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1"})

    def remaining(self):
        remaining = min(super().remaining(), self.work_deadline - time.monotonic())
        require(remaining > 0, "original work deadline reached; cleanup reserve retained")
        return remaining

    def _stop_owned(self, process, *, require_clean, deadline):
        # Core retains all original PID/group ownership checks. Only narrow its
        # wait to the existing driver's share of the original cleanup reserve.
        super()._stop_owned(process, require_clean=require_clean,
            deadline=min(deadline, self.allocation_deadline - 30))

    def save(self, name, raw):
        if name.startswith("http-"):
            # Core's final inventory independently adds all original HTTP files.
            return super().save(name, raw)
        require(self.process_journal is not None, "original journal required for local evidence")
        return self.process_journal.save("captures/" + name, raw, "installed-local-observation")

    def begin_process_capture(self):
        require(self.process_journal is None, "capture already initialized")
        journal = core.process_capture.CaptureJournal(self.root, reserve=self.reserve_capture)
        self.process_journal = journal
        self.controller_capture = core.process_capture.capture_controller(journal, self.allocation_deadline_ns,
            allocation_clock=self.allocation_clock)
        self.manifest_capture = journal.save("captures/original-manifest.json",
            self.original_manifest_raw, "installed-local-manifest")
        self.review_capture = journal.save("captures/original-review.json", self.review_raw, "installed-local-review")
        mount_raw = core.process_capture.read_bounded("/proc/self/mountinfo", 1024 * 1024)
        self.report["actual_mountinfo"] = journal.save("captures/original-mountinfo.txt", mount_raw, "local-mounts")
        runtime_path = Path(self.manifest["runtime_binding_review"])
        readonly_mount(runtime_path)
        runtime_raw = core.process_capture.read_bounded(runtime_path, 1024 * 1024)
        require(core.digest(runtime_raw) == self.manifest["runtime_binding_review_sha256"], "original static runtime receipt changed")
        contract.static_runtime(self.manifest, core.capture.strict_json_body(runtime_raw))
        self.report["static_runtime_binding"] = journal.save("captures/original-static-runtime.json",
            runtime_raw, "installed-static-runtime")
        refs = []
        for number, (relative, digest) in enumerate(self.inventory["input_sha256"].items()):
            raw = core.process_capture.read_bounded(ROOT / relative, 8 * 1024 * 1024)
            require(core.digest(raw) == digest, "original reviewed input changed")
            refs.append({"source": relative, "capture": journal.save(f"captures/input-{number:03d}.raw",
                raw, "installed-local-input")})
        self.reviewed_inputs_capture = journal.save("captures/reviewed-inputs.json", core.encoded({
            "format": "recall-installed-local-inputs/v1", "plan": self.inventory, "inputs": refs}), "local-plan")

    def json(self, method, path, space, body=None, *, role=None, status=200):
        response = self.http(method, path, role or "service-" + space, space, body)
        require(response["status"] == status, "original local HTTP request failed")
        return response["body"]

    def credentials(self):
        key = self.root / "private/signing.key"
        self.process(["service-token", "bootstrap", "--secret-file", str(key)])
        self.data_scopes = ["read:status", "read:space_config", "read:source", "write:source", "write:doc",
                            "read:doc:raw", "read:doc:metadata", "read:entity"]
        roles = [("operator", None, ["read:system", "write:system", "read:status"], False)]
        for space in self.corpus["spaces"]:
            roles.append(("service-" + space["id"], space, self.data_scopes, False))
            roles.append(("bootstrap-" + space["id"], space,
                          ["read:space_config", "write:space_config", "manage:grant", "manage:provider"], True))
        for role, space, scopes, bootstrap in roles:
            path = self.root / "private" / (role + ".token")
            arguments = ["service-token", "issue", "--secret-file", str(key), "--subject", "local-" + role,
                         "--expires-in", "3600", "--issuer", "installed-local", "--audience", "recall-local",
                         "--output", str(path)]
            if space is None:
                arguments += ["--global-admin"]
            else:
                clearances = self.root / "private" / (role + "-clearances.json")
                core.private_new(clearances, core.encoded([{"policy_id": space["policy"], "max_level": space["level"],
                                                           "labels": [], "handling_allow": []}]))
                arguments += ["--bootstrap-space" if bootstrap else "--space", space["id"],
                              "--clearances-file", str(clearances)]
            for scope in scopes:
                arguments += ["--scope", scope]
            self.process(arguments)
            require(not path.is_symlink() and path.stat().st_mode & 0o077 == 0, "private credential required")
            self.tokens[role] = path.read_text().strip()
            require(bool(self.tokens[role]), "installed CLI issued empty token")
        self.environment.update({"JWT_SECRET_FILE": str(key), "JWT_ALGORITHM": "HS256",
            "JWT_ISSUER": "installed-local", "JWT_AUDIENCE": "recall-local",
            "ELEPHANT_HOST": "127.0.0.1", "ELEPHANT_PORT": str(self.port)})
        self.record("fresh_cli_auth", roles=sorted(self.tokens), global_data_credentials=False)

    def launch(self, *, workers=False):
        arguments = ["serve", "--shutdown-timeout-seconds", "60"]
        if not workers:
            arguments.append("--server-only")
        self.process(arguments, serve=True)
        deadline = time.monotonic() + min(30, self.remaining())
        while time.monotonic() < deadline:
            require(self.child is not None and self.child.poll() is None, "owned local service exited")
            try:
                require(self.http("GET", "/v1/health")["status"] == 200, "local liveness failed")
                return
            except (ConnectionRefusedError, ConnectionResetError):
                time.sleep(min(1, self.remaining()))
        raise ValueError("bounded local startup expired")

    def assets(self, phase):
        # Work hashing and terminal hashing share the original clock. The outer
        # owner retains its final30s to stop the namespace process family.
        timeout = (self.remaining() if phase == "before" else self.cleanup_remaining() - 30)
        require(timeout > 0, "original model receipt deadline exhausted")
        with core.capture.total_deadline(timeout):
            readonly_mount(Path("/model-assets"))
            receipt = core.artifact.model_cache_receipt(Path("/model-assets"), self.manifest["binary_sha256"])
            require(receipt["files"] == self.cases["model_files"], "immutable model bytes/file set changed")
            for relative in receipt["files"]:
                readonly_mount(Path("/model-assets") / relative)
        self.save("model-cache-" + phase + ".json", core.encoded(receipt))
        if phase == "before":
            self.cache_before = receipt
        else:
            require(receipt == self.cache_before, "model cache changed during installed inference")
        self.report["model_cache_" + phase] = receipt

    def configure(self):
        self.launch()
        self.rpc("operator", None, "initialize", {"protocolVersion": "2025-03-26", "capabilities": {},
            "clientInfo": {"name": "installed-local", "version": "1"}})
        self.stop()
        self.environment.update({"ELEPHANT_DB": "surreal-db",
            "SURREALDB_URL": "rocksdb://" + str(self.root / "service/native-store"),
            "SURREALDB_NAMESPACE": "installed_local", "SURREALDB_DATABASE": "fresh_fixture",
            "SURREALDB_USER": "root", "SURREALDB_PASS": "synthetic-offline-only"})
        self.process(["migrate"])
        self.launch()
        status = self.tool("configure_runtime", {"ingest_enabled": True, "extractor_required": False,
            "multimodal_asset_root": str(self.root / "media")})
        require(status["configured"] and status["storage_ready"] and status["backend"] == "surreal-db",
                "actual native runtime unavailable")
        for space in self.corpus["spaces"]:
            name = space["id"]
            self.json("POST", "/v1/spaces", name, {"name": name, "security": contract.security(space),
                "embedding_provider": "fastembed", "embedding_model": contract.MODEL, "embedding_dims": 384,
                "purpose": space["guidance"], "processing": contract.processing(space)},
                role=self.bootstrap_role(name), status=201)
            self.json("PUT", f"/v1/spaces/{name}/grants/local-service-{name}", name,
                {"active": True, "scopes": self.data_scopes, "expected_generation": None},
                role=self.bootstrap_role(name))
            actual = self.json("GET", "/v1/spaces/" + name, name)
            contract.configuration(space, actual)
            self.configurations[name] = actual
            require(self.inventory_sources(name) == [], "local fixture space was not fresh")
        self.stop()
        self.launch(workers=True)
        self.check_configurations("before")
        self.record("fresh_local_profiles", spaces=list(self.configurations))

    def check_configurations(self, phase):
        observations = {}
        for space in self.corpus["spaces"]:
            name = space["id"]
            actual = self.json("GET", "/v1/spaces/" + name, name)
            contract.configuration(space, actual, self.configurations[name])
            response = self.http("GET", f"/v1/spaces/{name}/status", "service-" + name, name)
            contract.readiness(space, response)
            observations[name] = {"configuration": actual, "readiness": response}
        self.save("configurations-" + phase + ".json", core.encoded(observations))
        self.report["configurations_" + phase] = observations

    def inventory_sources(self, space):
        value = self.json("GET", f"/v1/spaces/{space}/sources?limit=100", space)
        require(value.get("next_cursor") is None and isinstance(value.get("sources"), list),
                "finite whole-space inventory incomplete")
        return value["sources"]

    def publish(self, source, deadline):
        name = source["space"]
        space = next(space for space in self.corpus["spaces"] if space["id"] == name)
        key = (name, source["id"])
        head_path = self.source_path(*key) + "/head"
        predecessor = self.current.get(key)
        if predecessor is not None:
            primary.original_operation(predecessor, self.json("GET", head_path, name), complete=True)
        request = primary.source_request(source, "surreal-db", self.manifest["nonce"],
            self.manifest["fixture_timestamp"], contract.security(space),
            predecessor["generation"] if predecessor else None)
        request["ingest_mode"] = "retrieval"
        request["doc"]["metadata"] = {"purpose": space["guidance"]}
        accepted = primary.acceptance(source, self.json("POST", "/v1/sources/ingest", name, request, status=202))
        self.report.setdefault("acceptances", []).append({"source": list(key), "revision": source["revision"],
            "original": accepted, "request_index": self.requests})
        while time.monotonic() < deadline:
            self.polls += 1
            require(self.polls <= self.cases["limits"]["publication_polls"], "original publication poll cap reached")
            operation = self.json("GET", "/v1/operations/" + quote(accepted["operation_id"], safe=""), name)
            primary.original_operation(accepted, operation)
            # Retryable receipts belong to the same original operation. Observe
            # the scheduler's bounded retries within the original deadline and
            # global poll cap; never resubmit or adopt a replacement operation.
            require(operation["state"] != "failed_final", "original local publication failed")
            if operation["state"] == "complete":
                primary.original_operation(accepted, operation, complete=True)
                break
            time.sleep(min(self.cases["limits"]["poll_seconds"], self.remaining(), max(0, deadline-time.monotonic())))
        else:
            raise ValueError("original publication deadline reached")
        head = self.json("GET", head_path, name)
        primary.original_operation(accepted, head, complete=True)
        path = "/v1/docs/" + quote(accepted["doc_id"], safe="")
        doc = self.json("GET", path, name)
        record = self.json("GET", "/v1/sources/" + quote(accepted["internal_source_id"], safe=""), name)
        pipeline = self.json("GET", path + "/pipeline-status", name)
        nodes = self.json("GET", path + "/chunks", name)
        rendered = next(row for row in self.rendered["sources"] if
                        (row["space"], row["source_key"], row["revision"]) == (*key, source["revision"]))
        contract.retrieval_publication(primary, source, accepted, doc, record, pipeline, nodes, space, rendered)
        revision = self.json("GET", path + "/revisions/" + quote(pipeline["revision"], safe=""), name)
        primary.revision_evidence(source, accepted, pipeline["revision"], revision, rendered,
            self.rendered["renderer"], {"source_security": contract.security(space)})
        self.publications.append({"source": list(key), "revision": source["revision"], "original": accepted,
            "operation": operation, "head": head, "pipeline": pipeline,
            "doc_sha256": primary.digest(doc), "immutable_revision_sha256": primary.digest(revision),
            "nodes_sha256": primary.digest(nodes)})
        self.current[key] = accepted

    def search(self):
        for transport in ("http", "mcp"):
            scores = []
            if transport == "mcp":
                for space in self.cases["spaces"]:
                    self.rpc("service-" + space, space, "initialize", {"protocolVersion": "2025-03-26",
                        "capabilities": {}, "clientInfo": {"name": "installed-local", "version": "1"}})
            for query in self.corpus["queries"]:
                request = primary.search_request(self.corpus, query, "semantic")
                name = query["space"]
                if transport == "http":
                    body = self.json("POST", f"/v1/spaces/{name}/search", name, request)
                else:
                    body = self.tool("search_scoped", request, token="service-" + name, space=name)
                original_row = {"id": query["id"], "space": name, "query": query["query"],
                    "response": body, "request_index": self.requests, "original_request": request,
                    "ranking_mode": "semantic", "fallback_fetches": 0, "query_rewrites": 0}
                self.query_rows[transport].append(original_row)
                row, scored = contract.query_observation(primary, semantic, self.corpus, query, body)
                original_row.update(row)
                original_row["oracle"] = scored
                scores.append(scored)
            positive = [row for row in scores if row["required_recall_at_3"] is not None]
            mrr = sum(row["reciprocal_rank"] for row in positive) / len(positive)
            self.report.setdefault("retrieval_scores", {})[transport] = {"macro_reciprocal_rank": mrr,
                "retrieval_pass": all(row["retrieval_pass"] for row in scores),
                "minimum_macro_reciprocal_rank": self.corpus["metric"]["min_macro_reciprocal_rank"]}
            require(all(row["retrieval_pass"] for row in scores)
                    and mrr >= self.corpus["metric"]["min_macro_reciprocal_rank"],
                    "original independent semantic ranking gate failed")

    def execute(self):
        self.assets("before")
        with core.capture.total_deadline(self.remaining()):
            artifact = core.artifact.verify(Path(self.manifest["archive"]), self.manifest["archive_sha256"],
                                            self.root / "installed")
        require(artifact["target"] == self.expected_target and artifact["source_commit"] == self.manifest["source_commit"]
                and artifact["binary_sha256"] == self.manifest["binary_sha256"] == core.file_digest(self.binary),
                "original installed source/target/binary differs")
        self.report["artifact_manifest"] = artifact
        prefix = self.process(["--version"])
        require((self.root / "captures" / (prefix + ".stdout")).read_text().strip()
                == "meerkat-recall " + artifact["version"], "installed version differs")
        self.record("verified_installed_binary")
        self.credentials()
        self.configure()
        deadline = min(self.work_deadline, time.monotonic() + self.cases["limits"]["publication_seconds"])
        for source in self.corpus["sources"]:
            require(time.monotonic() < deadline, "publication allocation exhausted")
            self.publish(source, deadline)
        for space in self.cases["spaces"]:
            rows = self.inventory_sources(space)
            require(len(rows) == len(primary.current_inventory(self.corpus, space))
                    and {row["external_source_id"]: row["desired_revision"] for row in rows}
                    == primary.current_inventory(self.corpus, space), "current source inventory differs")
            for row in rows:
                primary.original_operation(self.current[(space, row["external_source_id"])], row, complete=True)
        self.record("original_retrieval_publications", revisions=len(self.publications))
        self.report["local_inference"] = "observed: original local partition and all56 paragraph/chunk embeddings published"
        self.search()
        self.record("original_semantic_queries", http=24, mcp=24)
        home, business = self.cases["spaces"]
        request = primary.search_request(self.corpus, next(q for q in self.corpus["queries"] if q["space"] == business), "semantic")
        denied = self.http("POST", f"/v1/spaces/{business}/search", "service-" + home, business, request)
        require(denied["status"] == 403, "foreign signed space admitted")
        core.typed_denial(denied["body"], "forbidden_space", self.tokens.values())
        self.record("foreign_space_denied")
        negatives = [controls.negative_control(self.corpus, seed, 384) for seed in controls.SEEDS]
        require(all(row["negative_control_pass"] and row["product_acceptance"] is False for row in negatives),
                "original seeded fake-vector control did not reject")
        self.report["offline_negative_controls"] = negatives
        self.record("offline_fake_vectors_rejected")
        self.check_configurations("after")
        self.stop()
        require(core.file_digest(self.binary) == self.manifest["binary_sha256"], "installed binary changed")
        self.report["semantic_retrieval"] = "observed: independent numerical/evidence gates passed on both public transports"
        self.report["subset_observation"] = "passed; independent D installed verdict still required"

    def run(self):
        code = 0
        cleanup_complete = False
        try:
            self.begin_process_capture()
            self.execute()
        except Exception as error:
            code = 1
            self.report.update({"subset_observation": "failed", "failure_type": type(error).__name__,
                                "failure": str(error) if isinstance(error, ValueError) else "local prerequisite failed"})
        finally:
            # A failed after-cache receipt must not suppress original process
            # cleanup/capture. Attempt each terminal boundary independently.
            terminal = [("owned_stop", lambda: self.stop(require_clean=False)),
                        ("cache_after", lambda: self.assets("after") if self.cache_before is not None else None),
                        ("process_capture", self.finish_process_capture)]
            errors = []
            for phase, action in terminal:
                try:
                    action()
                except Exception as error:
                    errors.append({"phase": phase, "error_type": type(error).__name__})
            cleanup_complete = self.process_journal is not None and not errors and self.child is None and not self.owned_commands
            if errors:
                code = 1
                self.report.update({"subset_observation": "failed", "terminal_errors": errors})
            self.report.update({"duration_seconds": time.monotonic() - self.started, "http_attempts": self.requests,
                "processes": self.serial, "publication_polls": self.polls, "publications": self.publications,
                "queries": self.query_rows, "owned_cleanup_complete": cleanup_complete,
                "product_acceptance": False})
            if self.cleanup_remaining() <= 0:
                code = 1
                self.report["subset_observation"] = "failed: original cleanup deadline exceeded"
            raw = core.encoded(self.report)
            require(len(raw) <= 2 * 1024 * 1024, "bounded local observation exceeded")
            self.reserve_capture(len(raw))
            core.private_new(self.root / "observation.json", raw)
        return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("plan")
    run = commands.add_parser("run")
    run.add_argument("--manifest", type=Path, required=True)
    run.add_argument("--sha256", required=True)
    run.add_argument("--parent-network-namespace", required=True)
    run.add_argument("--execute-reviewed", action="store_true", required=True)
    args = parser.parse_args()
    if args.command == "plan":
        print(core.encoded(plan()).decode())
        return 0
    require(sys.version_info >= (3, 12), "artifact installation requires Python3.12+")
    manifest, inventory, raw, review = validate_manifest(args.manifest, args.sha256)
    isolation = core.offline_namespace(args.parent_network_namespace)
    core.enable_loopback()
    return LocalDriver(manifest, inventory, isolation, raw, review).run()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(core.encoded({"status": "unmet-prerequisite", "error_type": type(error).__name__,
                            "product_acceptance": False}).decode(), file=sys.stderr)
        sys.exit(78)
