#!/usr/bin/env python3
"""One installed seeded-negative run. Inert unless exact independent/root allocation is supplied."""
from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
from pathlib import Path
import re
import socket
import sys
import time
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import installed_fake_vectors_contract as contract
from installed_fake_vectors_spy import OwnedSpy
import primary_provider_driver_contract as primary


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


core = load("installed_seeded_core", HERE / "installed_linux_offline.py")
semantic = load("installed_seeded_oracle", ROOT / "tests/recall_preview_semantics/oracle.py")
CORPUS = ROOT / "tests/recall_preview_semantics/corpus.json"
RENDERED = ROOT / "tests/recall_preview/primary-rendered-map.json"
APPROVAL = ROOT / "tests/recall_preview/semantic-expectations-approval.json"
RECIPE = HERE / "installed_fake_vectors_recipe.json"
require = contract.require


def plan():
    corpus = core.read_json(CORPUS)
    semantic.validate_corpus(corpus)
    approval = core.read_json(APPROVAL)
    require(primary.digest(corpus) == primary.CORPUS_DIGEST
            and approval["corpus_canonical_sha256"] == primary.CORPUS_DIGEST
            and approval["status"] == "expectations-approved-before-execution"
            and approval["reviewer"] == "qualification (independent D lane)", "original independent corpus approval differs")
    require(core.file_digest(RENDERED) == primary.RENDERED_MAP_SHA256, "original independent rendered-map changed")
    primary.rendered_contract(core.read_json(RENDERED), corpus, core.file_digest(CORPUS))
    cases = core.read_json(RECIPE)
    require(cases["limits"] == contract.LIMITS and cases["seeds"] == list(contract.SEEDS)
            and cases["spy_limits"] == contract.SPY_LIMITS
            and cases["required_cases"] == list(contract.CASES)
            and cases["generator"] == contract.GENERATOR
            and cases["execution_authorized"] is False and cases["product_acceptance"] is False
            and cases["configurations"] == {k: list(v) for k, v in contract.CONFIGURATIONS.items()}
            and cases["sources"] == [[s["space"], s["id"], s["revision"]] for s in corpus["sources"]]
            and cases["queries"] == [q["id"] for q in corpus["queries"]], "frozen installed seeded recipe differs")
    paths = [Path(__file__), HERE / "installed_fake_vectors_contract.py", HERE / "installed_fake_vectors_spy.py",
             HERE / "installed_fake_vectors_test.py", HERE / "installed_fake_vectors_README.md", RECIPE,
             HERE / "installed_fake_vectors_manifest.example.json",
             HERE / "primary_provider_driver_contract.py", HERE / "installed_linux_offline.py", CORPUS, RENDERED, APPROVAL,
             ROOT / "tests/recall_preview_semantics/oracle.py", ROOT / "tests/recall_preview_semantics/full_workload_spy.py",
             ROOT / "tests/recall_preview_semantics/full_workload_transport.py", core.fixtures.CORPUS_PATH]
    # Raw source authorities for the actual serialized Document role and the
    # producer that creates it. These are qualification inputs, not Cargo work.
    paths += [ROOT / "crates/types/src/records/doc_node.rs",
              ROOT / "crates/pipeline/src/scheduler/evidence_work_kinds.rs"]
    hashes = {str(p.relative_to(ROOT)): core.file_digest(p) for p in paths}
    for module in core.LOADED_SOURCE_HASHES:
        hashes[str(Path(module.__file__).relative_to(ROOT))] = core.loaded_digest(module)
    return {"format": "recall-installed-fake-vectors-plan/v1", "expectations": cases,
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
    value = core.capture.strict_json_body(raw)
    fields = {"format", "execution_authorized", "backend", "source_commit", "archive", "archive_sha256",
              "binary_sha256", "run_directory", "expectation_review", "expectation_review_sha256",
              "nonce", "fixture_timestamp", "expires_at", "original_allocation_clock", "seed", "configuration",
              "external_provider_sends", "paid_model_calls", "fresh_spaces_authorized"}
    require(set(value) == fields and value["format"] == "recall-installed-fake-vectors-run/v1"
            and value["execution_authorized"] is True and value["backend"] in primary.BACKENDS
            and type(value["paid_model_calls"]) is int and value["paid_model_calls"] == 0
            and type(value["external_provider_sends"]) is int and value["external_provider_sends"] == 0,
            "exact isolated zero-paid-call allocation required")
    require(type(value["seed"]) is int and value["seed"] in contract.SEEDS
            and value["configuration"] in contract.CONFIGURATIONS, "undeclared seed/configuration")
    inventory = plan()
    require(value["fresh_spaces_authorized"] == inventory["expectations"]["spaces"], "both fresh named spaces need allocation")
    for field, width in (("source_commit", 40), ("archive_sha256", 64), ("binary_sha256", 64), ("expectation_review_sha256", 64)):
        require(isinstance(value[field], str) and re.fullmatch(f"[a-f0-9]{{{width}}}", value[field]), "exact identity absent:" + field)
    for field in ("archive", "run_directory", "expectation_review"):
        path = Path(value[field])
        require(path.is_absolute() and path == path.resolve(), "canonical explicit path required:" + field)
    require(re.fullmatch("[A-Za-z0-9_-]{16,64}", value["nonce"]), "fresh original nonce required")
    core.utc(value["fixture_timestamp"])
    clock, deadline = original_clock(value)
    require(clock["mono_after_ns"] <= time.monotonic_ns() < deadline - contract.LIMITS["cleanup_seconds"] * 10**9,
            "original allocation has no work window")
    review_raw = core.process_capture.read_bounded(value["expectation_review"], 1024 * 1024)
    require(core.digest(review_raw) == value["expectation_review_sha256"], "independent reviewed input changed")
    review = core.capture.strict_json_body(review_raw)
    require(review.get("format") == "recall-installed-fake-vectors-review/v1" and review.get("approved") is True
            and review.get("reviewer") == "qualification (independent D lane)" and review.get("plan") == inventory,
            "exact independent seeded plan review absent")
    return value, inventory, raw, review_raw


class InstalledFakeVectors(core.Driver):
    observation_format = "recall-installed-fake-vectors-observation/v1"

    def __init__(self, manifest, inventory, isolation, raw, review_raw):
        clock, deadline = original_clock(manifest)
        super().__init__(manifest, inventory, isolation, original_manifest_raw=raw)
        self.allocation_clock, self.allocation_deadline_ns = clock, deadline
        self.started = clock["mono_before_ns"] / 1e9
        self.allocation_deadline = core.process_capture.deadline_seconds(deadline)
        self.work_deadline = self.allocation_deadline - contract.LIMITS["cleanup_seconds"]
        self.review_raw = review_raw
        self.corpus, self.rendered = core.read_json(CORPUS), core.read_json(RENDERED)
        self.recipe = contract.recipe(self.corpus, manifest["seed"], manifest["configuration"])
        self.dimensions = dict(zip(self.cases["spaces"], contract.CONFIGURATIONS[manifest["configuration"]]))
        self.configurations, self.current, self.publications, self.query_rows = {}, {}, [], []
        self.polls = 0
        self.spy = None
        self.store_started = False
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            self.spy_port = reservation.getsockname()[1]
        require(self.spy_port != self.port, "API/spy endpoint collision")
        self.spy_endpoint = f"http://127.0.0.1:{self.spy_port}/v1"
        for space in self.corpus["spaces"]:
            self.environment[contract.credential_ref(space)] = contract.credential(space)
        self.report.update({"seed": manifest["seed"], "configuration": manifest["configuration"],
            "generator": contract.GENERATOR, "seeded_recipe": self.recipe, "harness_retries": 0,
            "query_rewrites": 0, "fallback_fetches": 0, "product_acceptance": False,
            "evidence_class": "installed-fake-vector-negative-control",
            "private_finalizer_receipt": "not exposed; original operation/head/publication captured"})

    def remaining(self):
        remaining = min(super().remaining(), self.work_deadline - time.monotonic())
        require(remaining > 0, "original work deadline reached; cleanup reserve retained")
        return remaining

    def _stop_owned(self, process, *, require_clean, deadline):
        super()._stop_owned(process, require_clean=require_clean, deadline=min(deadline, self.allocation_deadline - 20))

    def save(self, name, raw):
        if name.startswith("http-"):
            return super().save(name, raw)
        require(self.process_journal is not None, "original process journal required")
        return self.process_journal.save("captures/" + name, raw, "installed-fake-vectors")

    def begin_process_capture(self):
        require(self.process_journal is None, "capture already initialized")
        journal = core.process_capture.CaptureJournal(self.root, reserve=self.reserve_capture)
        self.process_journal = journal
        self.controller_capture = core.process_capture.capture_controller(journal, self.allocation_deadline_ns,
            allocation_clock=self.allocation_clock)
        self.manifest_capture = journal.save("captures/original-manifest.json", self.original_manifest_raw, "installed-manifest")
        self.review_capture = journal.save("captures/original-review.json", self.review_raw, "installed-review")
        references = []
        for index, (relative, expected) in enumerate(self.inventory["input_sha256"].items()):
            raw = core.process_capture.read_bounded(ROOT / relative, 8 * 1024**2)
            require(core.digest(raw) == expected, "original reviewed source/input changed")
            references.append({"source": relative, "capture": journal.save(f"captures/input-{index:03}.raw", raw, "seeded-input")})
        self.reviewed_inputs_capture = journal.save("captures/reviewed-inputs.json", core.encoded({
            "format": "recall-installed-seeded-inputs/v1", "plan": self.inventory, "inputs": references}), "seeded-plan")
        self.save("original-seeded-recipe.json", core.encoded(self.recipe))
        self.store_identity("before")

    def store_identity(self, phase):
        rows = []
        for relative in ("service/native-store", "service/state.snapshot"):
            path = self.root / relative
            require(not path.is_symlink() and path.resolve() == path, "store identity escaped fresh run")
            if phase == "before":
                require(not path.exists(), "negative fixture would reuse an existing store")
            row = {"relative_path": relative, "exists": path.exists()}
            if path.exists():
                stat = path.stat()
                row.update(device=stat.st_dev, inode=stat.st_ino, bytes=stat.st_size, directory=path.is_dir())
            rows.append(row)
        self.report["store_" + phase] = {"run_directory": str(self.root), "nonce": self.manifest["nonce"],
            "backend": self.manifest["backend"], "seed": self.manifest["seed"], "stores": rows}
        self.save("store-" + phase + ".json", core.encoded(self.report["store_" + phase]))

    def json(self, method, path, space, body=None, *, role=None, status=200):
        response = self.http(method, path, role or "service-" + space, space, body)
        require(response["status"] == status, "original seeded HTTP request failed")
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
            arguments = ["service-token", "issue", "--secret-file", str(key), "--subject", "seeded-" + role,
                         "--expires-in", "3600", "--issuer", "installed-seeded-negative", "--audience", "recall-seeded-negative",
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
            "JWT_ISSUER": "installed-seeded-negative", "JWT_AUDIENCE": "recall-seeded-negative",
            "ELEPHANT_HOST": "127.0.0.1", "ELEPHANT_PORT": str(self.port)})
        self.record("fresh_cli_auth", roles=sorted(self.tokens), global_data_credentials=False)

    def launch(self, *, workers=False):
        arguments = ["serve", "--shutdown-timeout-seconds", "60"]
        if not workers:
            arguments.append("--server-only")
        self.process(arguments, serve=True)
        deadline = time.monotonic() + min(30, self.remaining())
        while time.monotonic() < deadline:
            require(self.child is not None and self.child.poll() is None, "owned seeded service exited")
            try:
                require(self.http("GET", "/v1/health")["status"] == 200, "seeded liveness failed")
                return
            except (ConnectionRefusedError, ConnectionResetError):
                time.sleep(min(1, self.remaining()))
        raise ValueError("bounded seeded startup expired")

    def inventory_sources(self, space):
        value = self.json("GET", f"/v1/spaces/{space}/sources?limit=100", space)
        require(value.get("next_cursor") is None and isinstance(value.get("sources"), list),
                "finite whole-space inventory incomplete")
        return value["sources"]

    def publish(self, source, deadline):
        name = source["space"]
        space = next(space for space in self.corpus["spaces"] if space["id"] == name)
        key = (name, source["id"])
        head_path = self.source_path(*key)
        predecessor = self.current.get(key)
        if predecessor is not None:
            primary.original_operation(predecessor, self.json("GET", head_path, name), complete=True)
        request = primary.source_request(source, self.manifest["backend"], self.manifest["nonce"],
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
            require(operation["state"] not in ("failed_retryable", "failed_final"), "original seeded publication failed")
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
        contract.retrieval_publication(primary, source, accepted, doc, record, pipeline, nodes, space,
                                       rendered, self.dimensions[name], self.spy_endpoint)
        revision = self.json("GET", path + "/revisions/" + quote(pipeline["revision"], safe=""), name)
        primary.revision_evidence(source, accepted, pipeline["revision"], revision, rendered,
            self.rendered["renderer"], {"source_security": contract.security(space)})
        self.publications.append({"source": list(key), "revision": source["revision"], "original": accepted,
            "operation": operation, "head": head, "pipeline": pipeline,
            "doc_sha256": primary.digest(doc), "immutable_revision_sha256": primary.digest(revision),
            "nodes_sha256": primary.digest(nodes)})
        self.current[key] = accepted


    def configure(self):
        self.launch()
        self.rpc("operator", None, "initialize", {"protocolVersion": "2025-03-26", "capabilities": {},
            "clientInfo": {"name": "installed-seeded-negative", "version": "1"}})
        if self.manifest["backend"] == "elephant-db":
            status = self.tool("initialize_runtime", {"backend": "elephant-db", "ingest_enabled": True,
                "extractor_required": False, "snapshot_path": str(self.root / "service/state.snapshot"),
                "multimodal_asset_root": str(self.root / "media")})
        else:
            self.stop()
            self.environment.update({"ELEPHANT_DB": "surreal-db",
                "SURREALDB_URL": "rocksdb://" + str(self.root / "service/native-store"),
                "SURREALDB_NAMESPACE": "installed_negative", "SURREALDB_DATABASE": "fresh_fixture",
                "SURREALDB_USER": "root", "SURREALDB_PASS": "synthetic-offline-only"})
            self.process(["migrate"])
            self.launch()
            status = self.tool("configure_runtime", {"ingest_enabled": True, "extractor_required": False,
                "multimodal_asset_root": str(self.root / "media")})
        require(status["configured"] and status["storage_ready"] and status["backend"] == self.manifest["backend"],
                "actual selected persistent runtime unavailable")
        self.store_started = True
        for space in self.corpus["spaces"]:
            name, dimensions = space["id"], self.dimensions[space["id"]]
            self.json("POST", "/v1/spaces", name, {"name": name, "security": contract.security(space),
                "embedding_provider": "openai", "embedding_model": contract.model(dimensions),
                "embedding_dims": dimensions, "purpose": space["guidance"],
                "processing": contract.processing(space, dimensions, self.spy_endpoint)},
                role=self.bootstrap_role(name), status=201)
            self.json("PUT", f"/v1/spaces/{name}/grants/seeded-service-{name}", name,
                {"active": True, "scopes": self.data_scopes, "expected_generation": None},
                role=self.bootstrap_role(name))
            actual = self.json("GET", "/v1/spaces/" + name, name)
            contract.configuration(space, dimensions, self.spy_endpoint, actual)
            self.configurations[name] = actual
            require(self.inventory_sources(name) == [], "original negative fixture space was not empty")
        home, business = self.cases["spaces"]
        require(self.http("GET", f"/v1/spaces/{home}/sources?limit=100")["status"] == 401,
                "actual HTTP route admitted unauthenticated inventory")
        require(self.http("GET", f"/v1/spaces/{business}/sources?limit=100", "service-" + home, business)["status"] == 403,
                "actual named-space boundary admitted a foreign token")
        self.stop()
        self.spy = OwnedSpy(self.recipe, self.root / "spy", self.spy_port, self.work_deadline,
                            self.allocation_deadline - 10)
        self.spy.start()
        self.launch(workers=True)
        self.check_configurations("before")
        self.record("fresh_governed_negative_spaces")

    def check_configurations(self, phase):
        rows = {}
        for space in self.corpus["spaces"]:
            actual = self.json("GET", "/v1/spaces/" + space["id"], space["id"])
            contract.configuration(space, self.dimensions[space["id"]], self.spy_endpoint, actual,
                                   self.configurations[space["id"]])
            rows[space["id"]] = actual
        self.save("configurations-" + phase + ".json", core.encoded(rows))

    def search(self):
        scores = []
        for query in self.corpus["queries"]:
            request = primary.search_request(self.corpus, query, "semantic")
            body = self.json("POST", f'/v1/spaces/{query["space"]}/search', query["space"], request)
            row = {"id": query["id"], "space": query["space"], "query": query["query"],
                   "ranking_mode": "semantic", "original_request": request, "response": body,
                   "request_index": self.requests, "fallback_fetches": 0, "query_rewrites": 0}
            self.query_rows.append(row)
            row["oracle"] = contract.score(primary, semantic, self.corpus, query, body)
            scores.append(row["oracle"])
        verdict = contract.numerical_verdict(self.corpus, scores)
        self.report["numerical_verdict"] = verdict
        self.save("original-queries-and-numerical-scores.json", core.encoded(self.query_rows))
        require(verdict["numerical_retrieval_pass"] is False and verdict["negative_control_pass"] is True,
                "seeded installed route did not fail original numerical relevance thresholds")
        self.record("original_semantic_numerical_negative", **verdict)

    def verify_provider_inputs(self):
        snapshot, counts = self.spy.input_counts()
        expected = {(rule["space"], item["text"]): item for rule in self.recipe["rules"] for item in rule["output"]["mapping"]}
        require(set(counts) == set(expected), "actual provider calls omit or add original document/query text")
        for key, row in expected.items():
            require(counts[key] == 1 if row["kind"] == "query" else counts[key] >= 1,
                    "original query was repeated or document embedding omitted")
        self.report["provider_calls"] = snapshot
        self.report["provider_input_counts"] = [{"space": key[0], "text_sha256": primary.sha(key[1].encode()),
            "kind": expected[key]["kind"], "stream": expected[key]["stream"], "count": value}
            for key, value in sorted(counts.items())]
        self.save("original-physical-provider-calls.json", core.encoded({
            "calls": snapshot, "input_counts": self.report["provider_input_counts"]}))
        self.record("actual_seeded_document_and_query_embeddings")

    def execute(self):
        with core.capture.total_deadline(self.remaining()):
            artifact = core.artifact.verify(Path(self.manifest["archive"]), self.manifest["archive_sha256"],
                                            self.root / "installed")
        require(artifact["target"] == self.expected_target and artifact["source_commit"] == self.manifest["source_commit"]
                and artifact["binary_sha256"] == self.manifest["binary_sha256"] == core.file_digest(self.binary),
                "original installed source/target/binary differs")
        self.report["artifact_manifest"] = artifact
        self.record("verified_installed_binary")
        self.credentials()
        self.configure()
        deadline = min(self.work_deadline, time.monotonic() + self.cases["limits"]["publication_seconds"])
        for source in self.corpus["sources"]:
            require(time.monotonic() < deadline, "original publication allocation exhausted")
            self.publish(source, deadline)
        for space in self.cases["spaces"]:
            rows = self.inventory_sources(space)
            require(len(rows) == len(primary.current_inventory(self.corpus, space))
                    and {row["external_source_id"]: row["desired_revision"] for row in rows}
                    == primary.current_inventory(self.corpus, space), "current inventory differs from original corpus")
            for row in rows:
                primary.original_operation(self.current[(space, row["external_source_id"])], row, complete=True)
        self.report["publications"] = self.publications
        self.record("original_28_ordered_publications")
        self.search()
        self.verify_provider_inputs()
        self.check_configurations("after")
        self.stop()
        require(core.file_digest(self.binary) == self.manifest["binary_sha256"], "installed binary changed")
        require({row["case"] for row in self.report["cases"]} == set(self.cases["required_cases"]),
                "required original negative observations missing")
        self.report["subset_observation"] = "passed; independent D installed-negative verdict required"

    def run(self):
        code, errors = 0, []
        try:
            self.begin_process_capture()
            self.execute()
        except Exception as error:
            code = 1
            self.report.update({"subset_observation": "failed", "failure_type": type(error).__name__,
                "failure": str(error) if isinstance(error, ValueError) else "original prerequisite failed"})
        finally:
            def capture_spy():
                if self.spy is not None and self.spy.directory.is_dir():
                    references = []
                    for path in sorted(self.spy.directory.iterdir()):
                        require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 9 * 1024**2,
                                "invalid original spy capture")
                        references.append({"file": str(path.relative_to(self.root)), "sha256": core.file_digest(path),
                                           "bytes": path.stat().st_size})
                    self.save("spy-file-manifest.json", core.encoded(references))
            actions = [("owned_stop", lambda: self.stop(require_clean=False)),
                       ("spy_stop", lambda: self.spy.close() if self.spy else None),
                       ("spy_capture", capture_spy),
                       ("store_after", lambda: self.store_identity("after") if self.process_journal else None),
                       ("process_capture", self.finish_process_capture)]
            for phase, action in actions:
                try:
                    action()
                except Exception as error:
                    errors.append({"phase": phase, "error_type": type(error).__name__})
            if errors:
                code = 1
                self.report.update(subset_observation="failed", terminal_errors=errors)
            cleanup = self.process_journal is not None and not errors and self.child is None and not self.owned_commands
            cleanup = cleanup and (self.spy is None or self.spy.closed)
            if not cleanup:
                code = 1
            self.report.update(duration_seconds=time.monotonic() - self.started, http_attempts=self.requests,
                               processes=self.serial, cleanup_complete=bool(cleanup))
            with core.capture.total_deadline(max(0.001, self.cleanup_remaining())):
                raw = core.encoded(self.report)
                require(len(raw) <= 4 * 1024**2, "original negative observation envelope exceeded")
                self.reserve_capture(len(raw))
                core.private_new(self.root / "observation.json", raw)
            require(self.cleanup_remaining() > 0, "original whole clock exhausted during terminal seal")
        return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("plan")
    run = commands.add_parser("run")
    run.add_argument("--manifest", type=Path, required=True)
    run.add_argument("--manifest-sha256", required=True)
    run.add_argument("--execute-reviewed", action="store_true", required=True)
    run.add_argument("--parent-network-namespace", required=True)
    args = parser.parse_args()
    if args.command == "plan":
        print(json.dumps(plan(), indent=2))
        return 0
    require(sys.version_info >= (3, 12), "artifact installation requires Python 3.12+")
    manifest, inventory, raw, review = validate_manifest(args.manifest, args.manifest_sha256)
    isolation = core.offline_namespace(args.parent_network_namespace)
    core.enable_loopback()
    return InstalledFakeVectors(manifest, inventory, isolation, raw, review).run()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(json.dumps({"status": "unmet-prerequisite", "error_type": type(error).__name__,
                          "product_acceptance": False}), file=sys.stderr)
        sys.exit(78)
