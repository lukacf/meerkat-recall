#!/usr/bin/env python3
"""Capture public D28 Retrieval preparation; never self-certify a prepared fixture."""
from __future__ import annotations

import argparse
import asyncio
import base64
import datetime as dt
import importlib.util
import json
import os
from pathlib import Path
import re
import sys
import time
import urllib.parse
import uuid

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("preparation_workload_transport", ROOT / "scripts/qualification/recall_workload.py")
transport = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transport)
helpers = transport.helpers
require, encode, sha = helpers.require, transport.canonical, transport.sha
CASES = Path(__file__).with_name("workload_preparation_cases.json")
LIMITS = {"sources": 50, "sources_per_space": 25, "http_requests": 4096,
          "prepare_seconds": 3600, "request_seconds": 10, "close_seconds": 130,
          "response_bytes": 262144, "capture_bytes": 268435456,
          "embedding_nodes_per_source": 64, "source_text_bytes": 512}


def inventory():
    return {"format": "recall-workload-preparation-inventory/v1", "limits": LIMITS,
            "driver_sha256": helpers.file_digest(Path(__file__)),
            "cases_sha256": helpers.file_digest(CASES),
            "transport_sha256": helpers.file_digest(Path(transport.__file__)),
            "helpers_sha256": helpers.file_digest(Path(helpers.__file__)),
            "collector_sha256": helpers.file_digest(Path(helpers.capture.__file__)),
            "profiles_sha256": transport.inventory()["profiles_sha256"],
            "qualified": False, "product_acceptance": False}


def stable_configuration(value):
    result = dict(value)
    result.pop("updated_at", None)
    processing = dict(result["processing"])
    epoch = processing.pop("index_generation")
    require(type(epoch) is int and epoch >= 0, "invalid storage index generation")
    result["processing"] = processing
    return result


def source_request(source, backend, nonce, timestamp, envelope):
    """One retained typed Source+Doc request with explicitly reviewed envelopes."""
    identity = sha(encode([backend, nonce, source["space"], source["key"], source["revision"]]))
    def base(table):
        return {"id": table + ":" + str(uuid.uuid5(uuid.NAMESPACE_URL, "d28/" + identity + "/" + table)),
                "space_id": source["space"], "created_at": timestamp, "updated_at": timestamp,
                "created_by": "d28-qualification", "deleted_at": None,
                **json.loads(encode(envelope))}
    return {"source": {**base("source"), "type": "manual", "hash": source["sha256"],
                       "external_id": source["key"], "uri": source["source_url"], "metadata": {}},
            "doc": {**base("doc"), "title": source["key"], "text": source["text"],
                    "source": None, "sidecar_text": None, "media_attachments": [],
                    "rendition_key": f"d28/text/{source['key']}/{source['revision']}", "metadata": {}},
            "ingest_mode": "retrieval",
            "intake": {"revision_id": source["revision"], "idempotency_key": "d28-" + identity}}


def validate_sources(corpus):
    spaces = corpus["spaces"]
    require(len(spaces) == 2 and len({row["id"] for row in spaces}) == 2, "two distinct frozen spaces required")
    ids = {row["id"] for row in spaces}
    sources = corpus["sources"]
    require(len(sources) == 50 and len({(row["space"], row["key"]) for row in sources}) == 50,
            "exactly 50 distinct original sources required")
    for space in ids:
        require(sum(row["space"] == space for row in sources) == 25, "source distribution is not 25+25")
    for row in sources:
        require(row["space"] in ids and row["key"] and row["revision"], "unknown source identity")
        raw = row["text"].encode("utf-8")
        require(0 < len(raw) <= LIMITS["source_text_bytes"] and sha(raw) == row["sha256"],
                "frozen bounded source bytes changed")
        require("\n\n" not in row["text"] and not row["text"].startswith("#"),
                "reviewed short paragraph recipe changed")
    return sources


def acceptance(source, body):
    operation = body["operation"]
    for key in ("operation_id", "source_id", "source_generation"):
        require(isinstance(operation.get(key), str) and operation[key], "original acceptance identity absent")
    require(body.get("source_id") == operation["source_id"] and operation["space_id"] == source["space"],
            "acceptance belongs to another source or space")
    require(operation["desired_revision"] == source["revision"], "accepted revision differs")
    require(isinstance(body.get("doc_id"), str) and body["doc_id"].startswith("doc:"), "accepted immutable document absent")
    require(isinstance(body.get("work_item_id"), str) and body["work_item_id"], "original source work was not enqueued")
    return body


def operation_identity(source, accepted, value):
    original = accepted["operation"]
    for public, internal in (("operation_id", "operation_id"), ("internal_source_id", "source_id"),
                             ("generation", "source_generation")):
        require(value.get(public) == original[internal], "original source operation or generation changed")
    require(value.get("desired_revision") == source["revision"] and value.get("lifecycle") == "active",
            "source revision was superseded, retracted or erased")
    require(value.get("state") in ("pending", "running", "complete", "failed_retryable", "failed_final"),
            "unknown public operation state")


def published_inventory(source, accepted, observations, expected, rendered):
    """Check actual public fields, without inventing a private manifest/vector proof."""
    operation, head = observations["operation"], observations["head"]
    for value in (operation, head):
        operation_identity(source, accepted, value)
        require(value["state"] == "complete" and value["published_revision"] == source["revision"],
                "original revision is not completely published")
        require(value.get("error_code") is None, "completed publication still carries a failure")
    doc, record, nodes, pipeline = (observations[key] for key in ("doc", "source", "nodes", "pipeline"))
    require(doc["id"] == accepted["doc_id"] and doc["source"] == accepted["source_id"], "document mapping changed")
    require((rendered["space"], rendered["source_key"], rendered["revision"])
            == (source["space"], source["key"], source["revision"])
            and rendered["input_sha256"] == source["sha256"], "rendered expectation belongs to another original source")
    require(doc["title"] == source["key"] and doc["text"] == rendered["rendered_text"]
            and sha(doc["text"].encode()) == rendered["rendered_sha256"], "published rendered document bytes differ")
    require(record["id"] == accepted["source_id"] and record["external_id"] == source["key"], "external source mapping changed")
    require(record["hash"] == source["sha256"] and record.get("uri") == source["source_url"], "published source input identity differs")
    require(record["current_ingest_mode"] == "retrieval" and record["lifecycle_status"] == "retrieval_ready",
            "public source has not reached RetrievalReady")
    for row in (doc, record):
        require(row["space_id"] == source["space"] and row["security"] == expected["record_security"],
                "record envelope differs from independent expectation")
        require(row.get("deleted_at") is None, "published source or document is deleted")
        require(row.get("subjects") == [] and row.get("subjects_state") == "unknown",
                "published subject authority differs from the frozen input")
    revision = pipeline.get("revision")
    require(pipeline["doc_id"] == doc["id"] and isinstance(revision, str) and revision.startswith("doc_revision:"),
            "pipeline status is not for the original immutable document")
    immutable = observations.get("immutable_revision")
    require(isinstance(immutable, dict), "complete original immutable revision response missing")
    require(immutable.get("id") == revision and immutable.get("doc") == doc["id"]
            and immutable.get("space_id") == source["space"], "immutable revision identity or parent differs")
    require(immutable.get("status") == "active" and immutable.get("deleted_at") is None
            and immutable.get("supersedes") is None, "original immutable revision is not fresh and active")
    require(immutable.get("text") == rendered["rendered_text"]
            and immutable.get("text_hash") == rendered["rendered_sha256"], "immutable revision rendered bytes or hash differ")
    require(immutable.get("renderer") == {"name": "elephant.renderer.text", "version": "1.0.0", "config_hash": None},
            "immutable revision renderer differs from frozen default source")
    require(immutable.get("security") == expected["record_security"]
            and immutable.get("subjects") == [] and immutable.get("subjects_state") == "unknown",
            "immutable revision classified envelope differs")
    require(immutable.get("provenance", {}).get("sources") == [accepted["source_id"]],
            "immutable revision lacks the original source lineage")
    helpers.utc(immutable["rendered_at"])
    require(isinstance(nodes, list) and 0 < len(nodes) <= 128 and len({row["id"] for row in nodes}) == len(nodes),
            "public document node inventory missing, duplicated or unbounded")
    embeddable = []
    raw = rendered["rendered_text"].encode()
    require(len(raw) == rendered["rendered_utf8_bytes"], "rendered document byte length differs")
    spans = {"document": [], "paragraph": [], "chunk": []}
    for node in nodes:
        require(node["space_id"] == source["space"] and node["revision"] == revision
                and node["security"] == expected["record_security"] and node.get("deleted_at") is None,
                "node envelope or exact revision differs")
        require(node.get("subjects") == [] and node.get("subjects_state") == "unknown",
                "derived node changed the frozen subject authority")
        provenance = node["provenance"]
        require(provenance["sources"] == [accepted["source_id"]]
                and any(step.get("input") == revision for step in provenance["chain"]),
                "derived node lacks actual accepted source and revision lineage")
        require(node["role"] in spans, "node role is outside the frozen default tree recipe")
        start, end = node["span"]["start"], node["span"]["end"]
        require(type(start) is int and type(end) is int and 0 <= start < end <= len(raw), "invalid rendered UTF-8 span")
        spans[node["role"]].append(node["span"])
        if node["role"] not in ("chunk", "paragraph"):
            continue
        quote = raw[start:end].decode("utf-8")
        require(node.get("text") in (None, quote), "node text differs from its original source span")
        require(node.get("embedding_model") == expected["embedding_partition_model"], "node profile marker differs")
        embeddable.append(node)
    require(0 < len(embeddable) <= LIMITS["embedding_nodes_per_source"], "published batch exceeds reviewed 64-node cap")
    for role, wanted in (("document", [rendered["root_span"]]), ("paragraph", rendered["paragraph_spans"]),
                         ("chunk", rendered["chunk_spans"])):
        require(sorted(spans[role], key=lambda span: (span["start"], span["end"])) == wanted,
                "published " + role + " spans differ from independent rendered map")
    require(pipeline["chunks"] == {"total": len(embeddable), "embedded": len(embeddable)}
            and pipeline["nodes_count"] == len(nodes), "public embedding inventory markers are incomplete")
    return {"space": source["space"], "key": source["key"], "revision": source["revision"],
            "source_id": accepted["source_id"], "doc_id": doc["id"], "internal_doc_revision_id": revision,
            "operation_id": operation["operation_id"], "generation": operation["generation"],
            "source_sha256": source["sha256"], "embeddable_node_ids": [row["id"] for row in embeddable],
            "rendered_sha256": rendered["rendered_sha256"], "rendered_utf8_bytes": rendered["rendered_utf8_bytes"],
            "public_profile_marker": expected["embedding_partition_model"],
            "original_revision_record": {"id": revision, "text_sha256": immutable["text_hash"],
                "renderer": immutable["renderer"], "response_body_canonical_sha256": sha(encode(immutable))},
            "original_private_manifest": "unmet: not exposed by these public reads",
            "physical_vector_inventory": "unmet: node markers and completed operation are not a raw index inventory"}


def typed_contract(manifest, loaded):
    """Bind D's explicit representation delta without changing its allocation."""
    selected = transport.profiles.profile_for_manifest(manifest)
    contract, delta = loaded["preparation_contract"], loaded["typed_delta"]
    require(contract.get("format") == "recall-workload-typed-preparation/v1"
            and contract["corpus_sha256"] == manifest["corpus_sha256"], "independent typed contract differs")
    require(contract["top_level_fields"] == ["source", "doc", "ingest_mode", "intake"]
            and contract["ingest_mode"] == "retrieval", "typed submission recipe changed")
    require(contract["fixture_timestamp"] == manifest["fixture_timestamp"] == "2026-10-07T00:00:00Z"
            and contract["created_by"] == "d28-qualification", "frozen input timestamp or actor differs")
    require(contract["shared_input_base"] == {"deleted_at": None, "provenance": {"sources": [], "chain": []},
                "subjects": [], "subjects_state": "unknown"}
            and contract["doc"]["rendition_key_rule"] == "d28/text/{key}/{revision}", "frozen input envelope differs")
    policies = {row["id"]: {"policy_id": row["policy_id"], "level": row["level"], "labels": [], "handling": []}
                for row in loaded["corpus"]["spaces"]}
    require(contract["security_by_space"] == policies, "typed policies differ from independent corpus")
    require(delta.get("format") == "recall-workload-allocation-representation-delta/v1"
            and delta["base_allocation_sha256"] == manifest["provider_allocation_sha256"]
            and delta["preparation_contract_sha256"] == manifest["preparation_contract_sha256"]
            and delta["corpus_sha256"] == manifest["corpus_sha256"], "typed allocation delta identity differs")
    require(delta["override"] == {"preparation.representation": "typed Source+Doc envelope",
                "preparation.contract": selected.preparation_filename}, "unreviewed allocation override")


def cohort_begin(manifest, begin):
    require(isinstance(begin, dict) and len(encode(begin)) <= 65536, "invalid original G cohort begin receipt")
    require(begin.get("format") == "recall-exclusive-cohort-begin/v1" and begin.get("authority") == "exclusive_gateway_cohort"
            and begin.get("phase") == "preparation" and begin.get("backend") == manifest["backend"]
            and begin.get("allocation_sha256") == manifest["provider_allocation_sha256"], "wrong original G cohort")
    require(type(begin.get("cohort_id")) is int and begin["cohort_id"] > 0
            and re.fullmatch(r"[0-9a-f]{64}", begin.get("baseline_sha256", "")), "invalid original G cohort identity")


def cohort_close(begin, accounting, *, profile_id=transport.profiles.LEGACY_ID):
    selected = transport.profiles.profile(profile_id)
    require(isinstance(accounting, dict) and accounting.get("verdict") in ("passed", "failed", "unmet"), "invalid G accounting verdict")
    if accounting["verdict"] != "passed":
        return
    require(begin is not None and accounting.get("format") == "recall-exclusive-cohort-accounting/v1"
            and type(accounting.get("cohort_id")) is int
            and accounting.get("cohort_id") == begin["cohort_id"]
            and accounting.get("before_sha256") == begin["baseline_sha256"], "G close belongs to another original cohort")
    require(accounting.get("admission_closed") is True and type(accounting.get("unsettled_dispatches")) is int
            and accounting["unsettled_dispatches"] == 0
            and accounting.get("per_request_association") == "unmet", "G close is unsettled or invents request attribution")
    for name in ("after_sha256", "capture_inventory_sha256", "sealed_receipt_sha256"):
        require(re.fullmatch(r"[0-9a-f]{64}", accounting.get(name, "")), "G sealed evidence identity absent")
    count, reserved = accounting.get("physical_dispatch_reservations"), accounting.get("reserved_nano_usd")
    require(type(count) is int and 0 < count <= 450 and type(reserved) is int and 0 < reserved <= selected.money,
            "G preparation accounting exceeds frozen bounds or lacks physical evidence")
    require(reserved == count * 1_064_960, "physical attempt reservations differ from the frozen no-refund allocation")


def reviewed_inputs(path):
    manifest = helpers.read_json(path)
    require(manifest.get("format") == "recall-workload-preparation/v1" and manifest.get("execution_authorized") is True
            and manifest.get("provider_dispatch_authorized") is True, "preparation is not allocated")
    require(manifest.get("phase") == manifest.get("provider_phase") == "preparation"
            and manifest.get("backend") in ("elephant-db", "surreal-db"),
            "explicit backend preparation phase required")
    require(dt.datetime.now(dt.timezone.utc) < helpers.utc(manifest["expires_at"]), "allocation expired")
    require(re.fullmatch(r"[a-zA-Z0-9_-]{16,64}", manifest["nonce"]), "fresh bounded cohort nonce required")
    helpers.utc(manifest["fixture_timestamp"])
    transport.endpoint(manifest["endpoint"])
    for key, width in (("source_commit", 40), ("artifact_sha256", 64), ("binary_sha256", 64)):
        require(re.fullmatch(f"[0-9a-f]{{{width}}}", manifest[key]), "exact qualified artifact identity required")
    loaded = {}
    names = ("contract", "corpus", "provider_allocation", "typed_delta", "preparation_contract",
             "installed_runtime_evidence", "expected_configurations", "rendered_map")
    for name in ("review", *names):
        filename = Path(manifest[name + "_path"])
        require(filename.is_absolute() and helpers.file_digest(filename) == manifest[name + "_sha256"], f"{name} custody differs")
        loaded[name] = helpers.read_json(filename)
    review = loaded["review"]
    require(review.get("format") == "recall-workload-preparation-review/v1" and review.get("reviewer") == "qualification"
            and review.get("approved") is True, "independent preparation expectations not approved")
    for key in ("driver_sha256", "cases_sha256", "transport_sha256", "helpers_sha256", "collector_sha256"):
        require(review.get(key) == inventory()[key], f"reviewed {key} differs")
    for name in names:
        require(review.get(name + "_sha256") == manifest[name + "_sha256"], f"unreviewed {name}")
    selected = transport.profiles.profile_for_manifest(manifest)
    require(transport.profiles.profile_for_contract(loaded["contract"]) is selected, "preparation profile differs")
    if selected.id == transport.profiles.SEMANTIC140_ID:
        require(review.get("profiles_sha256") == transport.inventory()["profiles_sha256"], "closed-profile source is unreviewed")
    transport.validate_schedule(loaded["contract"], loaded["corpus"])
    require(loaded["contract"]["corpus_sha256"] == manifest["corpus_sha256"], "D corpus bytes changed")
    validate_sources(loaded["corpus"])
    loaded["rendered_by_source"] = transport.rendered_map_index(loaded["rendered_map"], loaded["corpus"], manifest["corpus_sha256"])
    typed_contract(manifest, loaded)
    runtime = loaded["installed_runtime_evidence"]
    require(runtime.get("qualified") is True, "actual installed runtime evidence has not been independently qualified")
    for key in ("source_commit", "artifact_sha256", "binary_sha256", "backend", "endpoint"):
        require(runtime.get(key) == manifest[key], f"installed runtime {key} differs")
    require(runtime.get("transient_retry_max_attempts") == 3 and runtime.get("embedding_max_total_http_attempts") == 3
            and runtime.get("document_tree_create_summaries") is False, "reviewed installed preparation recipe or retry configuration differs")
    expected = loaded["expected_configurations"]
    require(set(expected) == {row["id"] for row in loaded["corpus"]["spaces"]}, "expected named configurations differ")
    for row in loaded["corpus"]["spaces"]:
        item = expected[row["id"]]
        config = item["configuration"]
        require(config["space_id"] == row["id"] and config["embedding_provider"] == row["provider"]
                and config["embedding_model"] == row["model"] and config["embedding_dims"] == row["dimensions"],
                "reviewed space embedding identity differs from D corpus")
        require(config["security"]["policy_id"] == row["policy_id"] and config["security"]["level"] == row["level"],
                "reviewed named-space envelope differs")
        require(isinstance(item.get("record_security"), dict) and isinstance(item.get("embedding_partition_model"), str)
                and item["embedding_partition_model"].startswith("recall:"), "independent record/profile expectations missing")
        envelope = item["input_envelope"]
        require(set(envelope) == {"security", "subjects", "subjects_state", "provenance"}
                and envelope["security"] == item["record_security"], "explicit typed input envelope differs")
        require(item["record_security"] == {"policy_id": row["policy_id"], "level": row["level"], "labels": [], "handling": []},
                "source/document envelopes must retain the distinct frozen policies")
        require(envelope["subjects"] == [] and envelope["subjects_state"] == "unknown"
                and envelope["provenance"] == {"sources": [], "chain": []}, "unreviewed input subject/provenance authority")
    return manifest, loaded, transport.accounting_bridge(manifest, review)


class Preparation:
    def __init__(self, manifest, loaded, bridge):
        self.manifest, self.loaded, self.bridge = manifest, loaded, bridge
        self.host, self.port = transport.endpoint(manifest["endpoint"])
        self.root = Path(manifest["output_directory"])
        require(self.root.is_absolute() and self.root == self.root.resolve() and not self.root.exists(), "new canonical private output directory required")
        self.tokens = {}
        spaces = {row["id"] for row in loaded["corpus"]["spaces"]}
        require(set(manifest["space_credentials"]) == spaces, "explicit credentials for both named spaces required")
        require(set(manifest["credential_files"]) == set(manifest["space_credentials"].values()), "credential roles differ")
        for role, name in manifest["credential_files"].items():
            filename = Path(name)
            require(filename.is_absolute() and not filename.is_symlink() and filename.is_file()
                    and filename.stat().st_mode & 0o077 == 0 and filename.stat().st_size <= 16384, "bounded private credential file required")
            token = filename.read_text().strip()
            require(re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", token), "invalid signed credential file")
            self.tokens[role] = token
        self.root.mkdir(mode=0o700)
        (self.root / "captures").mkdir(mode=0o700)
        self.http_count = self.capture_bytes = self.posts = 0
        self.accepted, self.published = [], []
        self.provider_cohort_begin = None

    async def http(self, method, path, space, body=None):
        require(self.http_count < LIMITS["http_requests"], "preparation HTTP budget exhausted")
        require(dt.datetime.now(dt.timezone.utc) < helpers.utc(self.manifest["expires_at"]), "allocation expired")
        self.http_count += 1
        payload = b"" if body is None else encode(body)
        require(len(payload) <= 32768 and method in ("GET", "POST"), "unexpected preparation request")
        token = self.tokens[self.manifest["space_credentials"][space]]
        host = f"[{self.host}]:{self.port}" if ":" in self.host else f"{self.host}:{self.port}"
        request = (f"{method} {path} HTTP/1.1\r\nHost: {host}\r\nAuthorization: Bearer {token}\r\n"
                   f"X-Space-Id: {space}\r\nContent-Type: application/json\r\nAccept: application/json\r\n"
                   f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n").encode() + payload
        observed = {"raw_body": b"", "framing_complete": False, "body": None, "status": None}
        writer = None
        error = None
        started = time.monotonic_ns()
        intent = {"format": "recall-workload-preparation-request/v1", "request_index": self.http_count,
                  "method": method, "path": path, "space": space,
                  "credential_role": self.manifest["space_credentials"][space],
                  "request_body": body, "request_body_base64": base64.b64encode(payload).decode(),
                  "request_sha256": sha(payload), "started_monotonic_ns": started,
                  "harness_attempt": 1, "state": "intent-only; no physical-send claim"}
        intent_raw = encode(intent)
        # Reserve room for this intent and the bounded response before sending.
        # Failed recording cannot turn an uncertain submission into an uncaptured retry.
        response_reserve = 4 * LIMITS["response_bytes"] + 2 * len(payload) + 65536
        require(self.capture_bytes + len(intent_raw) + response_reserve <= LIMITS["capture_bytes"],
                "private preparation capture budget exhausted before dispatch")
        intent_path = self.root / "captures" / f"{self.http_count:04}.request.json"
        helpers.private_new(intent_path, intent_raw)
        # private_new syncs the file; also persist its name and the newly created
        # capture/output directory entries before a source mutation can leave us.
        for directory in (intent_path.parent, self.root, self.root.parent):
            descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        self.capture_bytes += len(intent_raw)
        require(dt.datetime.now(dt.timezone.utc) < helpers.utc(self.manifest["expires_at"]),
                "allocation expired while persisting original request")
        try:
            async with asyncio.timeout(LIMITS["request_seconds"]):
                reader, writer = await asyncio.open_connection(self.host, self.port, limit=transport.HEADER_CAP)
                writer.write(request)
                await writer.drain()
                await transport.read_frame(reader, observed)
        except (Exception, asyncio.CancelledError) as caught:
            error = caught
        finally:
            if writer is not None:
                writer.close()
                try:
                    async with asyncio.timeout(0.1):
                        await writer.wait_closed()
                except (Exception, asyncio.CancelledError) as caught:
                    error = error or caught
        raw = observed.pop("raw_body")[:LIMITS["response_bytes"]]
        response = {**observed, "raw_body_base64": base64.b64encode(raw).decode(), "body_sha256": sha(raw),
                    "bytes": len(raw), "raw_body_complete": observed["framing_complete"]}
        capture = {"method": method, "path": path, "space": space, "request_body": body,
                   "request_sha256": sha(payload), "response": response,
                   "request_index": self.http_count, "request_intent_sha256": sha(intent_raw),
                   "elapsed_ms": (time.monotonic_ns() - started) / 1e6,
                   "error_type": type(error).__name__ if error else None}
        encoded = encode(capture)
        self.capture_bytes += len(encoded)
        require(self.capture_bytes <= LIMITS["capture_bytes"], "private preparation capture budget exhausted")
        helpers.private_new(self.root / "captures" / f"{self.http_count:04}.json", encoded)
        if error:
            raise error
        require(response["framing_complete"] and response["body"] is not None, "incomplete preparation response")
        return response

    @staticmethod
    def head_path(source):
        return "/v1/spaces/" + urllib.parse.quote(source["space"], safe="") + "/sources/" + urllib.parse.quote(source["key"], safe="") + "/head"

    async def get(self, path, space):
        result = await self.http("GET", path, space)
        require(result["status"] == 200, "required original public observation unavailable")
        return result["body"]

    async def configurations(self):
        for space, expected in self.loaded["expected_configurations"].items():
            actual = await self.get("/v1/spaces/" + urllib.parse.quote(space, safe=""), space)
            require(stable_configuration(actual) == stable_configuration(expected["configuration"]),
                    "actual named-space authority changed")

    async def prepare(self):
        await self.configurations()
        sources = validate_sources(self.loaded["corpus"])
        # Check the entire fixture before the first mutation. Existing state is
        # not repaired, overwritten or treated as a resumed fresh allocation.
        for source in sources:
            absent = await self.http("GET", self.head_path(source), source["space"])
            require(absent["status"] == 404, "preparation source already exists or absence is unproved")
            helpers.typed_denial(absent["body"], "not_found", secrets=[row["text"] for row in sources])
        for source in sources:
            body = source_request(source, self.manifest["backend"], self.manifest["nonce"],
                                  self.manifest["fixture_timestamp"], self.loaded["expected_configurations"][source["space"]]["input_envelope"])
            self.posts += 1
            response = await self.http("POST", "/v1/sources/ingest", source["space"], body)
            require(response["status"] == 202, "original Retrieval acceptance failed; do not retry")
            accepted = acceptance(source, response["body"])
            self.accepted.append((source, accepted))
        pending = list(self.accepted)
        while pending:
            remaining = []
            for source, accepted in pending:
                operation = await self.get("/v1/operations/" + urllib.parse.quote(accepted["operation"]["operation_id"], safe=""), source["space"])
                operation_identity(source, accepted, operation)
                require(operation["state"] != "failed_final", "original source preparation failed terminally")
                if operation["state"] != "complete":
                    remaining.append((source, accepted))
                    continue
                doc_path = "/v1/docs/" + urllib.parse.quote(accepted["doc_id"], safe="")
                observations = {"operation": operation,
                    "head": await self.get(self.head_path(source), source["space"]),
                    "source": await self.get("/v1/sources/" + urllib.parse.quote(accepted["source_id"], safe=""), source["space"]),
                    "doc": await self.get(doc_path, source["space"]),
                    "nodes": await self.get(doc_path + "/chunks", source["space"]),
                    "pipeline": await self.get(doc_path + "/pipeline-status", source["space"])}
                revision = observations["pipeline"].get("revision")
                require(isinstance(revision, str) and revision.startswith("doc_revision:"), "original immutable revision absent")
                observations["immutable_revision"] = await self.get(
                    doc_path + "/revisions/" + urllib.parse.quote(revision, safe=""), source["space"])
                self.published.append(published_inventory(source, accepted, observations,
                    self.loaded["expected_configurations"][source["space"]],
                    self.loaded["rendered_by_source"][(source["space"], source["key"], source["revision"])]))
            pending = remaining
            if pending:
                await asyncio.sleep(1)
        await self.configurations()

    async def run(self):
        failure = None
        try:
            with helpers.capture.total_deadline(10):
                begin = self.bridge.authorize_provider_allocation(self.manifest, self.loaded["contract"], self.loaded["provider_allocation"])
            cohort_begin(self.manifest, begin)
            self.provider_cohort_begin = begin
            async with asyncio.timeout(LIMITS["prepare_seconds"]):
                await self.prepare()
        except (Exception, asyncio.CancelledError) as error:
            failure = type(error).__name__
        observation = {"format": "recall-workload-preparation-observation/v1", "phase": "preparation",
            **{key: self.manifest[key] for key in ("source_commit", "artifact_sha256", "binary_sha256", "backend", "endpoint", "nonce", "corpus_sha256", "contract_sha256", "preparation_contract_sha256", "provider_allocation_sha256", "typed_delta_sha256", "rendered_map_sha256")},
            "provider_cohort_begin": self.provider_cohort_begin, "http_requests": self.http_count,
            "acceptance_post_attempts": self.posts, "accepted": [accepted for _, accepted in self.accepted],
            "published_inventory": self.published, "source_count": len(self.published),
            "query_warmups": 0, "harness_retries": 0, "run_error_type": failure,
            "qualified": False, "product_acceptance": False,
            "proof_limits": ["public embedding markers are not raw physical vectors", "original private publication manifests are not returned by these APIs", "the complete immutable DocRevision is not returned by these APIs"],
            "capture_verdict": "passed" if failure is None and self.posts == len(self.published) == 50 else "failed"}
        if self.manifest.get("profile_id") == transport.profiles.SEMANTIC140_ID:
            observation["profile_id"] = transport.profiles.SEMANTIC140_ID
        try:
            with helpers.capture.total_deadline(LIMITS["close_seconds"]):
                accounting = self.bridge.provider_accounting(self.manifest, observation, self.loaded["provider_allocation"])
            cohort_close(self.provider_cohort_begin, accounting,
                         profile_id=transport.profiles.profile_for_manifest(self.manifest).id)
            observation["provider_accounting"] = accounting
        except Exception as error:
            observation["provider_accounting"] = {"verdict": "unmet", "error_type": type(error).__name__}
        if observation["provider_accounting"]["verdict"] != "passed":
            observation["capture_verdict"] = "failed"
        helpers.private_new(self.root / "observation.json", encode(observation))
        return 0 if observation["capture_verdict"] == "passed" else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("inventory")
    run = commands.add_parser("run")
    run.add_argument("--manifest", type=Path, required=True)
    run.add_argument("--execute-reviewed", action="store_true", required=True)
    args = parser.parse_args()
    if args.command == "inventory":
        print(encode(inventory()).decode())
        return 0
    require(sys.version_info >= (3, 11), "absolute async deadlines require Python 3.11+")
    return asyncio.run(Preparation(*reviewed_inputs(args.manifest)).run())


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(encode({"verdict": "unmet-prerequisite", "error_type": type(error).__name__, "qualified": False}).decode(), file=sys.stderr)
        sys.exit(78)
