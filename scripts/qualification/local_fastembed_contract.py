"""Pure installed local-inference contracts; no process, network or asset access."""
from __future__ import annotations

import json
import re
from pathlib import PurePosixPath

MODEL_DIGEST = "0e4372cdf7ebeabea95aa136b56d1d8856c3cf3a72e5224d7b00b05245ad4442"
MODEL = "bge-small-en-v1.5"
ENDPOINT = "file:///model-assets"
OPERATIONS = ("query_embedding", "document_embedding")
LIMITS = {"total_seconds": 1800, "cleanup_seconds": 90, "publication_seconds": 1200,
          "publication_polls": 600, "poll_seconds": 2, "http_calls": 1024,
          "http_seconds": 15, "snapshot_http_seconds": 15, "response_bytes": 262144,
          "processes": 16, "process_log_bytes": 1048576, "http_capture_bytes": 67108864}


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def static_runtime(manifest, receipt):
    """Attestation binds actual final build evidence; conversion is insufficient."""
    require(receipt.get("format") == "recall-installed-local-static-runtime/v1"
            and receipt.get("reviewer") in {"lifecycle", "gcp-recall-codex"} and receipt.get("reviewed") is True
            and receipt.get("actual_build") is True and receipt.get("link_mode") == "static"
            and receipt.get("ort_version") == "2.0.0-rc.12" and receipt.get("api") == 24
            and receipt.get("load_dynamic") is False, "actual static product runtime receipt absent")
    for key in ("source_commit", "archive_sha256", "binary_sha256"):
        require(receipt.get(key) == manifest[key], "static runtime belongs to another final artifact")
    features = receipt.get("ort_features", [])
    require(isinstance(features, list) and len(features) == len(set(features))
            and "api-24" in features and "download-binaries" in features
            and not {"load-dynamic", "disable-linking"}.intersection(features), "actual ORT feature identity differs")
    require(receipt.get("static_link_directive") == "cargo:rustc-link-lib=static=onnxruntime",
            "actual static ORT link directive absent")
    for key in ("feature_fingerprint_sha256", "link_output_sha256", "distribution_sha256"):
        require(isinstance(receipt.get(key), str) and re.fullmatch("[a-f0-9]{64}", receipt[key]),
                "actual native runtime build evidence digest absent: " + key)
    def reference(value):
        require(isinstance(value, dict) and isinstance(value.get("path"), str)
                and PurePosixPath(value["path"]).is_absolute() and ".." not in PurePosixPath(value["path"]).parts
                and isinstance(value.get("sha256"), str) and re.fullmatch("[a-f0-9]{64}", value["sha256"]),
                "original runtime evidence reference absent")
    reference(receipt.get("original_build_receipt"))
    reference(receipt.get("link_output"))
    require(receipt["link_output"]["sha256"] == receipt["link_output_sha256"], "runtime link reference differs")
    fingerprints = receipt.get("feature_receipts")
    require(isinstance(fingerprints, list) and len(fingerprints) == 2, "both actual ORT feature receipts required")
    for fingerprint in fingerprints:
        reference(fingerprint)
    require(len({row["path"] for row in fingerprints}) == 2
            and receipt["feature_fingerprint_sha256"] in {row["sha256"] for row in fingerprints},
            "original feature references differ")
    distribution = receipt.get("distribution", {})
    require(distribution.get("sha256") == receipt["distribution_sha256"]
            and isinstance(distribution.get("url"), str) and distribution["url"].startswith("https://"),
            "original pinned static distribution receipt absent")
    library = receipt.get("static_library", {})
    require(isinstance(library.get("sha256"), str) and re.fullmatch("[a-f0-9]{64}", library["sha256"])
            and type(library.get("bytes")) is int and 0 < library["bytes"] <= 512 * 1024 * 1024,
            "actual linked static archive identity absent")


def security(space):
    return {"policy_id": space["policy"], "level": space["level"], "labels": [], "handling": []}


def processing(space):
    name = space["id"]
    profile = name + "-bge384-text-v1"
    connection = name + "-local-assets"
    return {"generation": 1, "index_generation": 0, "policy_id": space["policy"],
            "connections": {connection: {"id": connection, "provider": "fastembed",
                "account_id": name + "-local-account", "endpoint": ENDPOINT,
                "credential_ref": None, "credential_generation": 1, "revoked": False}},
            "profiles": {profile: {"id": profile, "connection_id": connection,
                "model": MODEL, "model_version": "sha256:" + MODEL_DIGEST,
                "dimensions": 384, "preprocessing": "text-v1"}},
            "bindings": {operation: {"id": name + "-" + operation,
                "profile_id": profile, "revoked": False} for operation in OPERATIONS},
            "active_embedding_profile_id": profile, "pending_reindex": None}


def partition(space):
    # This order is the native VectorPartition serialization order. It is part
    # of the persisted embedding_model identity, unlike sorted capture JSON.
    return {"space_id": space["id"], "provider": "fastembed",
            "account_id": space["id"] + "-local-account", "endpoint": ENDPOINT,
            "profile_id": space["id"] + "-bge384-text-v1", "model": MODEL,
            "model_version": "sha256:" + MODEL_DIGEST, "dimensions": 384,
            "preprocessing": "text-v1"}


def partition_model(space):
    return "recall:" + json.dumps(partition(space), ensure_ascii=False, separators=(",", ":"))


def readiness(space, response):
    body = response["body"]
    # Overall readiness includes Graph generation, which this finite fixture
    # deliberately does not authorize. Preserve its actual 503/error in capture.
    require(response["status"] in (200, 503) and body["space"] == space["id"]
            and body["storage_ready"] is True and body["runtime_mode"] != "read_only"
            and body["processing_generation"] == 1, "selected local storage not ready")
    for operation in OPERATIONS:
        require(body["approved_operations"][operation] is True
                and body["available_operations"][operation] is True,
                "local embedding approval/assets unavailable")
        recipe = body["embedding_execution_recipes"][operation]
        require(recipe["partition"] == partition(space), "readiness selected another local model")
        require(recipe["provenance"]["operation"] == operation
                and recipe["provenance"]["config"]["space_id"] == space["id"]
                and recipe["provenance"]["config"]["generation"] == 1,
                "readiness recipe has foreign authority")
    for operation in ("extraction", "support_verification", "triage", "reindex_embedding"):
        require(body["approved_operations"][operation] is False, "unexpected operation approval")


def configuration(space, actual, previous=None):
    require(actual["space_id"] == space["id"] and actual["security"] == security(space)
            and actual["embedding_provider"] == "fastembed"
            and actual["embedding_model"] == MODEL and actual["embedding_dims"] == 384,
            "actual named-space authority/model differs")
    observed = dict(actual["processing"])
    index_generation = observed.pop("index_generation")
    expected = processing(space)
    expected.pop("index_generation")
    require(type(index_generation) is int and index_generation >= 0 and observed == expected,
            "local configuration or approvals changed")
    if previous is not None:
        # Index progress is storage-owned. All other configuration fields,
        # including extraction guidance, must survive the run unchanged.
        left, right = json.loads(json.dumps(previous)), json.loads(json.dumps(actual))
        for value in (left, right):
            value.pop("updated_at", None)
            value["processing"].pop("index_generation")
        require(left == right, "configuration changed during inference")


def retrieval_publication(primary, source, accepted, doc, record, pipeline, nodes, space, rendered):
    require(doc["id"] == accepted["doc_id"] and doc["source"] == accepted["internal_source_id"]
            and doc["text"] == rendered["rendered_text"], "original immutable document differs")
    require(record["id"] == accepted["internal_source_id"] and record["external_id"] == source["id"]
            and record["hash"] == primary.sha(source["text"].encode())
            and record["current_ingest_mode"] == "retrieval"
            and record["lifecycle_status"] == "retrieval_ready", "Retrieval publication absent")
    for item in (doc, record):
        require(item["space_id"] == space["id"] and item.get("deleted_at") is None
                and item["security"] == security(space), "publication authority differs")
    revision = pipeline["revision"]
    require(pipeline["doc_id"] == accepted["doc_id"] and isinstance(revision, str)
            and revision.startswith("doc_revision:"), "original pipeline revision absent")
    require(isinstance(nodes, list) and len(nodes) == 3
            and len({node["id"] for node in nodes}) == 3, "original three-node inventory differs")
    spans = {role: [] for role in ("document", "paragraph", "chunk")}
    raw = rendered["rendered_text"].encode()
    for node in nodes:
        require(node["space_id"] == space["id"] and node["revision"] == revision
                and node["security"] == security(space) and node.get("deleted_at") is None,
                "indexed node authority or revision differs")
        require(node["role"] in spans, "unexpected indexed node role")
        span = node["span"]
        start, end = span["start"], span["end"]
        require(type(start) is int and type(end) is int and 0 <= start < end <= len(raw),
                "invalid immutable UTF-8 span")
        require(node.get("text") in (None, raw[start:end].decode()), "indexed text differs")
        spans[node["role"]].append(span)
        if node["role"] in ("paragraph", "chunk"):
            require(node.get("embedding_model") == partition_model(space), "wrong indexed local partition")
    require(spans == {"document": [rendered["root_span"]], "paragraph": rendered["paragraph_spans"],
                      "chunk": rendered["chunk_spans"]}, "published immutable span coverage differs")
    require(pipeline["chunks"] == {"total": 2, "embedded": 2} and pipeline["nodes_count"] == 3,
            "actual local embedding coverage incomplete")


def query_observation(primary, oracle, corpus, query, response):
    require(response.get("partial") is False, "partial semantic response")
    coverage = response.get("coverage", {})
    for key, revision in primary.current_inventory(corpus, query["space"]).items():
        item = coverage.get(key, {})
        require(item.get("state") == "complete"
                and all(item.get(field) == revision for field in
                        ("requested_revision", "desired_revision", "published_revision")),
                "whole current source coverage incomplete")
    row = {"id": query["id"], "space": query["space"], "query": query["query"],
           "fallback_fetches": 0, "query_rewrites": 0, "response": response}
    scored = oracle.score_case(corpus, query, row, None)
    # The original oracle requires an independent text review for groundedness.
    # Keep that requirement unmet; additionally enforce its mechanical evidence
    # checks here so a right ID with a forged quote cannot pass retrieval alone.
    mechanical = [error for error in scored["grounding_errors"]
                  if error != "independent response-bound semantic review missing"
                  and error != "required answer facts missing from independently reviewed output"]
    require(not mechanical and scored["completion_pass"], "invalid original source evidence/coverage")
    require(all(item["kind"] == "chunk" for item in response["results"]),
            "Retrieval-only fixture returned unreviewed canonical content")
    return row, scored
