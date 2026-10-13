"""Independent, offline scoring of original public responses, never a search engine.

No provider, network, document rescue, query rewriting or mutable thresholds live
here. The reviewer attestation is input evidence, not authentication proof.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

CORPUS = Path(__file__).with_name("corpus.json")
# Identity appointed by the separate D expectation seal. This checks the declared
# reviewer identity only; authentication and custody still require D review.
APPROVED_REVIEWER = "qualification (independent D lane)"


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def text_digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def load_corpus(path: Path = CORPUS) -> dict:
    corpus = json.loads(path.read_text())
    validate_corpus(corpus)
    return corpus


def evidence_id(source: dict) -> str:
    return f'{source["id"]}@{source["revision"]}#0'


def validate_corpus(corpus: dict) -> None:
    """Validate authored expectations; this is never a product verdict."""
    def require(condition: bool, message: str) -> None:
        # Validation must remain active under python -O as well as normal use.
        if not condition:
            raise ValueError(f"invalid semantic corpus: {message}")

    spaces = {space["id"] for space in corpus["spaces"]}
    require(len(spaces) == len(corpus["spaces"]) == 2, "expected two unique spaces")
    require(corpus["metric"]["cutoff"] == 3, "cutoff must remain three")
    seen, current = set(), set()
    for source in corpus["sources"]:
        key = (source["space"], evidence_id(source))
        require(source["space"] in spaces and key not in seen, "unknown space or duplicate source revision")
        seen.add(key)
        require(bool(source["text"]) and len(source["text"].encode()) < 2000, "source text size")
        if source.get("state") != "superseded":
            identity = (source["space"], source["id"])
            require(identity not in current, "multiple current revisions")
            current.add(identity)
    evidence = {(e["space"], e["id"]): e for e in corpus["evidence"]}
    require(len(evidence) == len(corpus["evidence"]), "duplicate evidence identity")
    sources = {(s["space"], s["id"], s["revision"]): s for s in corpus["sources"]}
    for passage in evidence.values():
        source = sources[(passage["space"], passage["source_id"], passage["revision"])]
        require(0 <= passage["start"] < passage["end"] <= len(source["text"].encode()), "evidence bounds")
        require(source["text"].encode()[passage["start"]:passage["end"]].decode() == passage["quote"], "evidence quote")
    ids = set()
    for query in corpus["queries"]:
        require(query["id"] not in ids and query["space"] in spaces, "query identity or space")
        ids.add(query["id"])
        require(bool(query["query"].strip()) and len(query["required"]) <= 3, "query or expected source count")
        require(len(query["required"]) == len(set(query["required"])), "duplicate expected evidence")
        require(not set(query["facts"]) & set(query["forbidden_facts"]), "contradictory fact expectations")
        for expected in query["required"]:
            require((query["space"], expected) in evidence, "unknown expected evidence")
        if query.get("unsupported_question"):
            require(not query["required"] and not query["facts"], "unsupported question has required answer")
    for counterexample in corpus["unsupported_claims"]:
        require(any(key[1] == counterexample["evidence"] for key in evidence), "counterexample evidence")
        require(bool(counterexample["claim"]) and bool(counterexample["reason"]), "counterexample rationale")


def _index_unique(rows: list, key: str, errors: list[str], label: str) -> dict:
    result = {}
    if not isinstance(rows, list):
        errors.append(f"{label}: expected array")
        return result
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get(key), str):
            errors.append(f"{label}: malformed identity")
            continue
        if row[key] in result:
            errors.append(f"{label}: duplicate {row[key]}")
        result[row[key]] = row
    return result


def _review_strings(value: Any, errors: list[str], label: str) -> set[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        errors.append(f"review {label} must be an array of nonempty strings")
        return set()
    return set(value)


def score_case(corpus: dict, query: dict, observation: dict, review: dict | None) -> dict:
    """Rank source-revisions by FIRST appearance across the unchanged result list.

    Every evidence reference is checked, including beyond the cutoff. A claim
    citing two sources contributes two consecutive source ranks in wire order.
    Missing ranks stay in the P@3 denominator. No relevance rescue is possible.
    """
    retrieval_errors, grounding_errors, completion_errors = [], [], []
    if observation.get("query") != query["query"] or observation.get("space") != query["space"]:
        retrieval_errors.append("original query or selected space changed")
    if observation.get("fallback_fetches") != 0:
        retrieval_errors.append("fallback fetch count missing or nonzero")
    response = observation.get("response", {})
    if not isinstance(response, dict):
        response = {}
        retrieval_errors.append("response is not an object")
    results = response.get("results")
    if not isinstance(results, list):
        results = []
        retrieval_errors.append("results missing or not an array")
    if len(results) > corpus["metric"]["cutoff"]:
        retrieval_errors.append("result count exceeds the requested HTTP result limit")
    sources = {(s["id"], s["revision"]): s for s in corpus["sources"]
               if s["space"] == query["space"]}
    passages = [e for e in corpus["evidence"] if e["space"] == query["space"]]
    ranked, rank_support, verified_by_result = [], {}, {}
    for index, result in enumerate(results):
        if not isinstance(result, dict) or result.get("kind") not in ("claim", "entity", "chunk"):
            retrieval_errors.append(f"result {index}: invalid kind")
            continue
        if not isinstance(result.get("text"), str):
            grounding_errors.append(f"result {index}: missing text")
        refs = result.get("evidence")
        verified_by_result[index] = set()
        verified_quotes = set()
        if not isinstance(refs, list) or not refs:
            grounding_errors.append(f"result {index}: no immutable evidence")
            continue
        for ref in refs:
            if not isinstance(ref, dict):
                grounding_errors.append(f"result {index}: malformed evidence")
                continue
            pair = (ref.get("source_id"), ref.get("revision"))
            if not all(isinstance(value, str) and value for value in pair):
                grounding_errors.append(f"result {index}: malformed source revision identity")
                continue
            # Rank from actual wire references BEFORE deciding whether relevant.
            if pair not in ranked:
                ranked.append(pair)
                rank_support[pair] = set()
                first_occurrence = True
            else:
                first_occurrence = False
            source = sources.get(pair)
            if source is None:
                grounding_errors.append(f"result {index}: foreign/unknown source revision")
                continue
            if source.get("state") == "superseded":
                grounding_errors.append(f"result {index}: superseded source in current answer")
            span = ref.get("span")
            if not isinstance(span, dict) or set(span) != {"start", "end"}:
                grounding_errors.append(f"result {index}: missing byte span")
                continue
            start, end = span["start"], span["end"]
            raw = source["text"].encode()
            if type(start) is not int or type(end) is not int or not 0 <= start < end <= len(raw):
                grounding_errors.append(f"result {index}: invalid byte range")
                continue
            try:
                quote = raw[start:end].decode("utf-8")
            except UnicodeDecodeError:
                grounding_errors.append(f"result {index}: split UTF-8 boundary")
                continue
            if quote != ref.get("quote"):
                grounding_errors.append(f"result {index}: quote differs from immutable fixture")
                continue
            if "quote_hash" in ref and ref["quote_hash"] != text_digest(quote):
                grounding_errors.append(f"result {index}: incorrect quote hash")
                continue
            verified_quotes.add(quote)
            supported = {e["id"] for e in passages
                         if (e["source_id"], e["revision"]) == pair
                         and start <= e["start"] and end >= e["end"]}
            verified_by_result[index].update(supported)
            # A later supporting span cannot rescue an earlier irrelevant
            # first occurrence of this source, even outside the scoring cutoff.
            if first_occurrence:
                rank_support[pair] = supported
        if result.get("kind") == "chunk" and (not isinstance(result.get("text"), str)
                                              or result["text"] not in verified_quotes):
            grounding_errors.append(f"result {index}: chunk text is not its immutable quote")
    k = corpus["metric"]["cutoff"]
    expected = set(query["required"])
    expected_pairs = {(e["source_id"], e["revision"]) for e in passages if e["id"] in expected}
    hits = sum(bool(rank_support[pair] & expected) for pair in ranked[:k])
    precision = hits / k
    minimum_precision = len(expected) / k
    required_recall = hits / len(expected) if expected else None
    first = next((i + 1 for i, pair in enumerate(ranked)
                  if rank_support[pair] & expected), None)
    reciprocal_rank = 1 / first if first else 0.0
    if expected and (hits != len(expected) or precision < minimum_precision):
        retrieval_errors.append("required evidence missing from fixed top-3 ranks")
    # Unrelated supported search hits are not invented answers. The independent
    # review must instead reject any unsupported query-answering proposition.

    # Independent reviewer maps ACTUAL returned text to predeclared fact atoms.
    # Copying expected fact IDs into a capture is insufficient without this
    # separate, response-bound review. It is still an attestation, not a signature.
    if not isinstance(review, dict) or review.get("response_sha256") != digest(response):
        grounding_errors.append("independent response-bound semantic review missing")
        judgements = []
    else:
        judgements = review.get("claims", [])
        if not isinstance(judgements, list):
            grounding_errors.append("review claims must be an array")
            judgements = []
    seen_indices, facts = set(), set()
    for judgement in judgements:
        if not isinstance(judgement, dict):
            grounding_errors.append("review contains malformed claim judgment")
            continue
        index = judgement.get("result_index")
        if type(index) is not int or index in seen_indices or not 0 <= index < len(results):
            grounding_errors.append("review has duplicate/unknown result index")
            continue
        seen_indices.add(index)
        result = results[index]
        if not isinstance(result, dict) or not isinstance(result.get("text"), str):
            grounding_errors.append("review targets malformed result text")
            continue
        if judgement.get("text_sha256") != text_digest(result["text"]):
            grounding_errors.append("review text digest differs from returned result")
        if judgement.get("verdict") != "supported":
            grounding_errors.append("independent review found unsupported/contradicted claim")
        cited = _review_strings(judgement.get("evidence_ids", []), grounding_errors, "evidence_ids")
        if not cited or not cited <= verified_by_result.get(index, set()):
            grounding_errors.append("review cites evidence absent from its own result")
        result_facts = _review_strings(judgement.get("facts", []), grounding_errors, "facts")
        facts.update(result_facts)
        if result_facts & set(query["forbidden_facts"]):
            grounding_errors.append("forbidden canonical claim asserted")
    for index, result in enumerate(results):
        if isinstance(result, dict) and result.get("kind") in ("claim", "entity") and index not in seen_indices:
            grounding_errors.append(f"result {index}: canonical content lacks independent review")
    if not set(query["facts"]) <= facts:
        grounding_errors.append("required answer facts missing from independently reviewed output")

    coverage = response.get("coverage")
    if not isinstance(coverage, dict):
        completion_errors.append("coverage must be object keyed by external source ID")
        coverage = {}
    if response.get("partial") is not False:
        completion_errors.append("response incomplete or partial flag missing")
    # Every returned source needs truthful current coverage too, including
    # distractors. A negative query cannot omit coverage for its actual hits.
    for source_id, revision in expected_pairs | set(ranked):
        item = coverage.get(source_id, {})
        if not isinstance(item, dict):
            completion_errors.append(f"{source_id}: malformed coverage item")
            continue
        if any(item.get(key) != revision for key in ("requested_revision", "desired_revision", "published_revision")):
            completion_errors.append(f"{source_id}: requested/desired/published mismatch")
        if item.get("state") != "complete":
            completion_errors.append(f"{source_id}: coverage not complete")
    return {"id": query["id"], "precision_at_3": precision,
            "minimum_precision_at_3": minimum_precision, "required_recall_at_3": required_recall,
            "reciprocal_rank": reciprocal_rank, "retrieval_pass": not retrieval_errors,
            "groundedness_pass": not grounding_errors, "completion_pass": not completion_errors,
            "retrieval_errors": retrieval_errors, "grounding_errors": grounding_errors,
            "completion_errors": completion_errors}


def evaluate(corpus: dict, capture: dict, review: dict | None = None) -> dict:
    """Score all cases, independently retain eligibility and execution limitations."""
    validate_corpus(corpus)
    if not isinstance(capture, dict) or (review is not None and not isinstance(review, dict)):
        raise ValueError("capture and review must be objects")
    errors = []
    cases = _index_unique(capture.get("cases", []), "id", errors, "capture")
    expected_ids = {query["id"] for query in corpus["queries"]}
    if set(cases) != expected_ids:
        errors.append("capture query set differs from frozen corpus (missing/extra cases)")
    review = review or {}
    approved = (review.get("status") == "approved" and review.get("reviewer") == APPROVED_REVIEWER
                and review.get("corpus_sha256") == digest(corpus)
                and review.get("capture_sha256") == digest(capture))
    if not approved:
        errors.append("independent D review missing or not bound to exact corpus/capture")
    reviews = _index_unique(review.get("cases", []), "id", errors, "review") if approved else {}
    reports = [score_case(corpus, query, cases.get(query["id"], {}), reviews.get(query["id"]))
               for query in corpus["queries"]]
    positive = [r for r in reports if r["required_recall_at_3"] is not None]
    mrr = sum(r["reciprocal_rank"] for r in positive) / len(positive)
    manifest = capture.get("manifest", {})
    if not isinstance(manifest, dict):
        errors.append("manifest must be an object")
        manifest = {}
    if manifest.get("evidence_class") != "real-provider":
        errors.append("deterministic spy/fake-vector/fixture evidence is not semantic acceptance")
    for key in ("source_sha", "artifact_sha256", "configuration_sha256", "provider_receipts_sha256"):
        value = manifest.get(key, "")
        length = 40 if key == "source_sha" else 64
        if not isinstance(value, str) or len(value) != length or any(c not in "0123456789abcdef" for c in value):
            errors.append(f"missing/invalid exact identity: {key}")
    if manifest.get("corpus_sha256") != digest(corpus):
        errors.append("manifest corpus mismatch")
    if manifest.get("backend") not in ("surreal-db", "elephant-db"):
        errors.append("unknown backend")
    if manifest.get("authentication_enabled") is not True:
        errors.append("authentication not enabled")
    if manifest.get("expectations_review_sha256") != review.get("expectations_review_sha256") or not review.get("expectations_review_sha256"):
        errors.append("missing independent pre-execution expectation approval")
    if manifest.get("query_mode") != "semantic-only":
        errors.append("semantic-only run required; lexical/hybrid success cannot validate vectors")
    if manifest.get("fallback_fetches") != 0 or manifest.get("query_rewrites") != 0:
        errors.append("fallback fetches or query rewrites not explicitly zero")
    retrieval_pass = all(r["retrieval_pass"] for r in reports) and mrr >= corpus["metric"]["min_macro_reciprocal_rank"]
    groundedness_pass = all(r["groundedness_pass"] for r in reports)
    completion_pass = all(r["completion_pass"] for r in reports)
    return {"corpus_sha256": digest(corpus), "capture_sha256": digest(capture),
            "verdict": "pass" if not errors and retrieval_pass and groundedness_pass and completion_pass else "not-qualified",
            "evidence_class": manifest.get("evidence_class", "missing"), "eligibility_errors": errors,
            "retrieval_pass": retrieval_pass, "groundedness_pass": groundedness_pass,
            "completion_pass": completion_pass, "macro_reciprocal_rank": mrr, "cases": reports,
            "limits": ["Reviewer and capture provenance are attestations requiring D custody review.",
                       "This run qualifies one named backend/artifact, not other artifacts or consumer adoption."]}
