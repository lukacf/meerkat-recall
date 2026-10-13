"""Pure installed negative-control contracts; never a provider-quality verdict."""
from __future__ import annotations

import hashlib
import json
import math
import struct

SEEDS = (17, 219, 20261006)
GENERATOR = "semantic-index-negative-v1"
CONFIGURATIONS = {"local384": (384, 384), "primary": (1536, 3072)}
CORPUS_SHA256 = "fa6a84149483d3511fd559ac9a13e89d6c1c041ae97d6f15084b52b7d7f21c1b"
CREDENTIAL = "synthetic-installed-fake-vector-only"
LIMITS = {"total_seconds": 1800, "cleanup_seconds": 90, "publication_seconds": 1200,
          "publication_polls": 600, "poll_seconds": 2, "http_calls": 1024,
          "http_seconds": 15, "snapshot_http_seconds": 15, "response_bytes": 262144,
          "processes": 18, "process_log_bytes": 1048576, "http_capture_bytes": 134217728}
SPY_LIMITS = {"owned_spy_dispatches_per_backend": 256, "owned_spy_request_bytes": 131072,
              "owned_spy_response_bytes": 8388608, "owned_spy_concurrency": 16,
              "held_response_seconds": 15}
CASES = ("verified_installed_binary", "fresh_cli_auth", "fresh_governed_negative_spaces",
         "original_28_ordered_publications", "original_semantic_numerical_negative",
         "actual_seeded_document_and_query_embeddings")


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def encode(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(encode(value)).hexdigest()


def vector(seed, stream, dimensions):
    """Match the Rust counter generator, including its two f32 roundings."""
    require(type(seed) is int and seed in SEEDS and type(stream) is int and 0 <= stream < 20000
            and type(dimensions) is int and dimensions in (384, 1536, 3072), "unreviewed vector identity")
    f32 = lambda value: struct.unpack("<f", struct.pack("<f", value))[0]
    values = []
    for coordinate in range(dimensions):
        raw = hashlib.sha256(GENERATOR.encode() + struct.pack("<QQQ", seed, stream, coordinate)).digest()
        value = int.from_bytes(raw[:8], "little") >> 11
        values.append(f32(value / (1 << 53) * 2.0 - 1.0))
    norm = math.sqrt(sum(value * value for value in values))
    require(math.isfinite(norm) and norm > 0, "invalid generated norm")
    return [f32(value / norm) for value in values]


def security(space):
    return {"policy_id": space["policy"], "level": space["level"], "labels": [], "handling": []}


def model(dimensions):
    return f"installed-seeded-negative-{dimensions}"


def credential(space):
    return CREDENTIAL + "-" + space["id"]


def credential_ref(space):
    return "INSTALLED_FAKE_VECTOR_" + space["id"].replace("-", "_").upper()


def partition(space, dimensions, endpoint):
    # Persisted VectorPartition JSON ordering is deliberately not sorted.
    return {"space_id": space["id"], "provider": "openai", "account_id": space["id"] + "-negative",
            "endpoint": endpoint, "profile_id": space["id"] + "-negative",
            "model": model(dimensions), "model_version": GENERATOR,
            "dimensions": dimensions, "preprocessing": "text-v1"}


def partition_model(space, dimensions, endpoint):
    return "recall:" + json.dumps(partition(space, dimensions, endpoint), ensure_ascii=False, separators=(",", ":"))


def processing(space, dimensions, endpoint):
    name = space["id"] + "-negative"
    return {"generation": 1, "index_generation": 0, "policy_id": space["policy"],
            "connections": {name: {"id": name, "provider": "openai", "account_id": name,
                "endpoint": endpoint, "credential_ref": credential_ref(space),
                "credential_generation": 1, "revoked": False}},
            "profiles": {name: {"id": name, "connection_id": name, "model": model(dimensions),
                "model_version": GENERATOR, "dimensions": dimensions, "preprocessing": "text-v1"}},
            "bindings": {op: {"id": space["id"] + "-" + op, "profile_id": name, "revoked": False}
                         for op in ("document_embedding", "query_embedding")},
            "active_embedding_profile_id": name, "pending_reindex": None}


def recipe(corpus, seed, configuration):
    require(digest(corpus) == CORPUS_SHA256 and seed in SEEDS and type(seed) is int
            and configuration in CONFIGURATIONS, "original corpus/seed/configuration differs")
    rules = []
    for space, dimensions in zip(corpus["spaces"], CONFIGURATIONS[configuration]):
        mapping = []
        for ordinal, source in enumerate(corpus["sources"]):
            if source["space"] == space["id"]:
                mapping.append({"text": source["text"], "stream": ordinal, "kind": "document",
                                "identity": [source["id"], source["revision"]]})
        for ordinal, query in enumerate(corpus["queries"]):
            if query["space"] == space["id"]:
                mapping.append({"text": query["query"], "stream": 10000 + ordinal,
                                "kind": "query", "identity": [query["id"]]})
        require(len({row["text"] for row in mapping}) == len(mapping), "ambiguous original text/ordinal mapping")
        rules.append({"id": space["id"] + "-seeded", "space": space["id"], "model": model(dimensions),
            "path": "/v1/embeddings", "credential_sha256": hashlib.sha256(credential(space).encode()).hexdigest(),
            "required_text": [], "forbidden_text": [], "maximum_calls": 128,
            "hold_calls": [], "statuses": [200] * 128, "mode": "embedding",
            "output": {"dimensions": dimensions, "seed": seed, "mapping": mapping}})
    return {"format": "recall-installed-seeded-spy/v1", "generator": GENERATOR,
            "corpus_sha256": CORPUS_SHA256, "seed": seed, "configuration": configuration, "rules": rules}


def validate_recipe(value, expected_sha256, corpus):
    require(digest(value) == expected_sha256, "original seeded recipe hash changed")
    require(value == recipe(corpus, value["seed"], value["configuration"]), "seeded recipe is not the closed original mapping")
    return json.loads(encode(value))


def response(rule, body):
    require(set(body) <= {"input", "model", "dimensions", "encoding_format", "user"}
            and body.get("model") == rule["model"], "unexpected original embedding request")
    inputs = body.get("input")
    require(type(inputs) is list and 1 <= len(inputs) <= 128
            and all(type(text) is str for text in inputs), "bounded original text batch required")
    output = rule["output"]
    require(body.get("dimensions", output["dimensions"]) == output["dimensions"]
            and body.get("encoding_format", "float") == "float", "embedding dimension/format substitution")
    mapping = {row["text"]: row for row in output["mapping"]}
    require(all(text in mapping for text in inputs), "undeclared embedding text; no normalization or fallback")
    return "application/json", encode({"object": "list", "model": body["model"],
        "data": [{"index": index, "object": "embedding",
                  "embedding": vector(output["seed"], mapping[text]["stream"], output["dimensions"])}
                 for index, text in enumerate(inputs)], "usage": {"prompt_tokens": 1, "total_tokens": 1}})


def configuration(space, dimensions, endpoint, actual, previous=None):
    require(actual["space_id"] == space["id"] and actual["security"] == security(space)
            and actual["embedding_provider"] == "openai" and actual["embedding_model"] == model(dimensions)
            and actual["embedding_dims"] == dimensions and actual.get("purpose") == space["guidance"],
            "original test-only space identity changed")
    observed, expected = dict(actual["processing"]), processing(space, dimensions, endpoint)
    generation = observed.pop("index_generation")
    expected.pop("index_generation")
    require(type(generation) is int and generation >= 0 and observed == expected, "governed test-only bindings changed")
    if previous is not None:
        def stable(value):
            value = json.loads(encode(value))
            value.pop("updated_at", None)
            value["processing"].pop("index_generation")
            return value
        require(stable(actual) == stable(previous), "configuration changed during original run")


def score(primary, oracle, corpus, query, actual):
    require(actual.get("partial") is False and isinstance(actual.get("results"), list)
            and len(actual["results"]) == 3, "negative control needs complete actual top-three retrieval")
    expected = primary.current_inventory(corpus, query["space"])
    require(set(actual.get("coverage", {})) == set(expected), "whole original inventory coverage differs")
    for key, revision in expected.items():
        row = actual["coverage"][key]
        require(row.get("state") == "complete" and all(row.get(field) == revision for field in
                ("requested_revision", "desired_revision", "published_revision")), "source coverage incomplete")
    require(all(row.get("kind") == "chunk" for row in actual["results"]), "unreviewed canonical search result")
    result = oracle.score_case(corpus, query, {"id": query["id"], "space": query["space"],
        "query": query["query"], "fallback_fetches": 0, "response": actual}, None)
    ignored = {"independent response-bound semantic review missing",
               "required answer facts missing from independently reviewed output"}
    require(result["completion_pass"] and not [error for error in result["grounding_errors"] if error not in ignored],
            "invalid mechanical evidence cannot count as a numerical negative")
    return result


def numerical_verdict(corpus, scores):
    require(len(scores) == 24 and [row["id"] for row in scores] == [q["id"] for q in corpus["queries"]],
            "all original numerical cases required in order")
    positive = [row for row in scores if row["required_recall_at_3"] is not None]
    require(bool(positive), "original positive queries absent")
    mrr = sum(row["reciprocal_rank"] for row in positive) / len(positive)
    numerical_pass = all(row["retrieval_pass"] for row in scores) and mrr >= corpus["metric"]["min_macro_reciprocal_rank"]
    return {"numerical_retrieval_pass": numerical_pass, "macro_reciprocal_rank": mrr,
            "negative_control_pass": not numerical_pass, "product_acceptance": False,
            "evidence_class": "installed-fake-vector-negative-control"}


def retrieval_publication(primary, source, accepted, doc, record, pipeline, nodes, space, rendered, dimensions, endpoint):
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
            require(node.get("embedding_model") == partition_model(space, dimensions, endpoint), "wrong indexed seeded partition")
    require(spans == {"document": [rendered["root_span"]], "paragraph": rendered["paragraph_spans"],
                      "chunk": rendered["chunk_spans"]}, "published immutable span coverage differs")
    require(pipeline["chunks"] == {"total": 2, "embedded": 2} and pipeline["nodes_count"] == 3,
            "actual governed embedding coverage incomplete")
