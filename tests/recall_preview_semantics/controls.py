"""Seeded fake-vector control; offline diagnostic, never a provider or route stub.

Only ranks are fabricated for the negative control. No expected document is
injected into a successful result and no product acceptance can result from it.
"""
from __future__ import annotations

import math
import random

from oracle import evidence_id, score_case

SEEDS = (17, 219, 20261006)
DIMENSIONS = (8, 384, 1536)


def normalized_random(rng: random.Random, dimensions: int) -> list[float]:
    vector = [rng.uniform(-1.0, 1.0) for _ in range(dimensions)]
    norm = math.sqrt(sum(value * value for value in vector))
    return [value / norm for value in vector]


def evidence(source: dict) -> dict:
    return {"source_id": source["id"], "revision": source["revision"],
            "span": {"start": 0, "end": len(source["text"].encode())}, "quote": source["text"]}


def negative_control(corpus: dict, seed: int, dimensions: int) -> dict:
    rng = random.Random(seed)
    sources = [s for s in corpus["sources"] if s.get("state") != "superseded"]
    vectors = [(source, normalized_random(rng, dimensions)) for source in sources]
    shape_valid = all(len(vector) == dimensions and all(math.isfinite(v) for v in vector)
                      and abs(sum(v * v for v in vector) - 1.0) < 1e-9
                      and max(vector) > min(vector) for _, vector in vectors)
    reports = []
    for query in corpus["queries"]:
        query_vector = normalized_random(rng, dimensions)
        ranked = sorted(((sum(a * b for a, b in zip(vector, query_vector)), source)
                         for source, vector in vectors if source["space"] == query["space"]),
                        key=lambda row: row[0], reverse=True)
        response = {"results": [{"kind": "chunk", "text": source["text"],
                                  "evidence": [evidence(source)], "score": score}
                                 for score, source in ranked[:3]], "coverage": {}, "partial": False}
        reports.append(score_case(corpus, query, {"query": query["query"], "space": query["space"],
                                                "fallback_fetches": 0, "response": response}, None))
    positive = [row for row in reports if row["required_recall_at_3"] is not None]
    mrr = sum(row["reciprocal_rank"] for row in positive) / len(positive)
    numerical_retrieval_pass = (all(row["retrieval_pass"] for row in reports)
                               and mrr >= corpus["metric"]["min_macro_reciprocal_rank"])
    return {"evidence_class": "offline-fake-vector-negative-control", "seed": seed,
            "dimensions": dimensions, "valid_finite_nonzero_unit_vectors": shape_valid,
            "macro_reciprocal_rank": mrr, "numerical_retrieval_pass": numerical_retrieval_pass,
            "negative_control_pass": shape_valid and not numerical_retrieval_pass,
            "product_acceptance": False,
            "failed_query_ids": [row["id"] for row in reports if not row["retrieval_pass"]],
            "limits": "No product/provider was exercised. Real-provider acceptance additionally requires this control applied to the installed semantic-only index."}
