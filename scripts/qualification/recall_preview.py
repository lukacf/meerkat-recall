#!/usr/bin/env python3
"""Offline inventory/evaluation and bounded actual public-surface capture.

Capture never rewrites queries, fetches known documents, starts a service, sends
mutations, executes a build or makes an unsolicited provider probe. Explicit
--dispatch-governed-provider is required to call a search that may invoke a model.
"""

from __future__ import annotations

import argparse
import base64
from contextlib import contextmanager
import http.client
import json
import math
import os
from pathlib import Path
import re
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tests" / "recall_preview"))
import fixtures  # noqa: E402
import matrix  # noqa: E402
import oracle  # noqa: E402


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise oracle.Rejected("redirect refused; service origin must remain explicit")


@contextmanager
def total_deadline(seconds: float):
    """Bound the entire sequential POSIX capture, including trickling responses."""
    oracle.require(seconds > 0 and hasattr(signal, "setitimer"), "positive POSIX capture deadline required")
    oracle.require(signal.getitimer(signal.ITIMER_REAL)[0] == 0, "capture cannot replace another active deadline")
    prior = signal.getsignal(signal.SIGALRM)

    def expired(_signal, _frame):
        raise TimeoutError("capture exceeded agreed total time budget")

    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, prior)


def request_json(url: str, token: str, payload: dict, timeout: float, max_bytes: int) -> dict:
    request = urllib.request.Request(url, data=json.dumps(payload).encode(), headers={
        "Authorization": f"Bearer {token}", "Content-Type": "application/json",
        "Accept": "application/json",
    }, method="POST")
    started = time.monotonic()
    with total_deadline(timeout):
        try:
            stream = urllib.request.build_opener(NoRedirect).open(request, timeout=timeout)
        except urllib.error.HTTPError as response:
            stream = response
        with stream:
            error = None
            try:
                body = stream.read(max_bytes + 1)
            except http.client.IncompleteRead as incomplete:
                body = incomplete.partial
                error = "incomplete HTTP response framing"
            lengths = stream.headers.get_all("Content-Length", [])
            transfers = stream.headers.get_all("Transfer-Encoding", [])
            if transfers and (lengths or len(transfers) != 1 or transfers[0].strip().lower() != "chunked"):
                error = "ambiguous or unsupported HTTP response framing"
            if lengths and (len(lengths) != 1 or re.fullmatch(r"[0-9]+", lengths[0].strip()) is None
                            or int(lengths[0].strip()) != len(body)):
                error = "incomplete or invalid HTTP Content-Length"
            if len(body) > max_bytes:
                error = "response exceeded agreed byte budget"
            complete = error is None
            # Preserve at most the agreed bytes, including a failed response's
            # prefix. A prefix hash must never be presented as a complete body.
            captured = body[:max_bytes]
            duration_ms = (time.monotonic() - started) * 1000
            oracle.require(duration_ms <= timeout * 1000, "response exceeded agreed total time budget")
            parsed = None
            if complete:
                try:
                    parsed = strict_json_body(captured)
                except (ValueError, UnicodeError) as invalid:
                    error = f"invalid JSON response: {invalid}"
            result = {"status": stream.status, "body": parsed, "bytes": len(body),
                      "duration_ms": duration_ms, "body_sha256": oracle.sha256(captured),
                      "raw_body_base64": base64.b64encode(captured).decode("ascii"),
                      "raw_body_complete": complete, "framing_complete": complete}
            if error is not None:
                result["capture_error"] = error
            return result


def strict_json_body(body: bytes):
    def object_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON object key")
            result[key] = value
        return result

    def invalid_constant(_value):
        raise ValueError("non-finite JSON number")

    def finite_float(value):
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("non-finite JSON number")
        return parsed

    return json.loads(body.decode("utf-8"), object_pairs_hook=object_pairs,
                      parse_constant=invalid_constant, parse_float=finite_float)


def normalize_search(query: dict, response: dict) -> dict:
    """Adapt the public revision-search contract without repairing missing coverage.

    Hashes not exposed by this wire version are explicitly oracle-computed. The
    exact returned quote/offsets/revision still must agree with the frozen corpus.
    """
    oracle.require(response.get("framing_complete") is True, "incomplete HTTP response cannot qualify retrieval")
    oracle.require(not response.get("capture_error"), "invalid captured response cannot qualify retrieval")
    body = response["body"]
    oracle.require(isinstance(body, dict), "response must be a JSON object")
    oracle.require(response["status"] == 200, f"HTTP search failed ({response['status']})")
    oracle.require(body.get("partial") is False, "partial search cannot qualify complete retrieval")
    by_key = body.get("coverage")
    oracle.require(isinstance(by_key, dict), "missing exact-revision coverage object")
    for constraint in query["required"]:
        row = by_key.get(constraint["key"])
        oracle.require(row is not None and row.get("requested_revision") == constraint["revision"], "requested revision coverage absent")
        oracle.require(row.get("state") == "complete", "requested revision not indexed")
    known = {(source["space"], source["key"], source["revision"]): source for source in fixtures.corpus()["sources"]}
    rows = body.get("results")
    oracle.require(isinstance(rows, list), "missing results array")
    oracle.require(len(rows) <= query["k"], "public result count exceeds requested bound")
    # Score source coverage within the original top-k public results. Multiple
    # evidence spans do not manufacture additional independent document ranks.
    results: dict[tuple, dict] = {}
    for hit in rows:
        oracle.require(hit.get("kind") in {"claim", "entity", "chunk"}, "unknown result kind")
        oracle.nonempty(hit.get("text"), "result text")
        spans = hit.get("evidence")
        oracle.require(isinstance(spans, list) and bool(spans), "result lacks evidence array")
        for row in spans:
            key, revision = row["source_id"], row["revision"]
            source = known.get((query["space"], key, revision))
            oracle.require(source is not None, "unknown result source revision")
            state = by_key.get(key)
            oracle.require(state is not None, "result omitted source coverage")
            evidence = {"key": key, "revision": revision, "space": query["space"],
                        "start": row["span"]["start"], "end": row["span"]["end"], "quote": row["quote"],
                        "internal_doc_revision_id": row.get("internal_doc_revision_id"),
                        "quote_hash": row.get("quote_hash", oracle.sha256(row["quote"].encode())),
                        "revision_hash": row.get("revision_hash", oracle.sha256(source["text"].encode())),
                        "hash_origin": "wire-if-present-otherwise-oracle"}
            result = results.setdefault((key, revision), {"key": key, "revision": revision,
                "space": query["space"], "published_revision": state["published_revision"],
                "desired_revision": state["desired_revision"], "processing_state": state["state"], "evidence": []})
            result["evidence"].append(evidence)
    oracle.require(len(results) <= query["k"], "source metric requires an agreed cutoff for multi-source results")
    return {"query": query["query"], "space": query["space"], "k": query["k"],
            "outcome": "complete", "evaluation_fallbacks": [], "results": list(results.values())}


def capture(args: argparse.Namespace) -> dict:
    oracle.require(args.dispatch_governed_provider, "search capture requires explicit provider-dispatch authorization")
    parsed = urllib.parse.urlsplit(args.url)
    oracle.require(parsed.scheme in {"http", "https"} and parsed.hostname and not parsed.username and not parsed.password and not parsed.query and not parsed.fragment, "explicit service origin required")
    oracle.require(parsed.scheme == "https" or parsed.hostname in {"127.0.0.1", "localhost", "::1"}, "plain HTTP is limited to loopback")
    token = oracle.nonempty(os.environ.get(args.token_env), "service token environment variable")
    oracle.require(args.timeout > 0 and args.max_bytes > 0, "positive agreed capture budgets required")
    oracle.require(re.fullmatch(r"[0-9a-f]{40}", args.source_sha) is not None, "exact source SHA required")
    oracle.require(re.fullmatch(r"[0-9a-f]{64}", args.artifact_sha256) is not None, "exact artifact digest required")
    oracle.require(args.backend in matrix.BACKENDS, "unknown backend")
    selected = [query for query in fixtures.corpus()["queries"] if query["space"] == args.space]
    oracle.require(bool(selected), "named fixture space required")
    captures = []
    for query in selected:
        payload = {"query": query["query"], "limit": query["k"], "filters": {},
                   "source_revision_constraints": {row["key"]: row["revision"] for row in query["required"]}}
        route = "/v1/spaces/" + urllib.parse.quote(args.space, safe="") + "/search"
        try:
            response = request_json(args.url.rstrip("/") + route, token, payload, args.timeout, args.max_bytes)
        except (OSError, ValueError) as error:
            captures.append({"query_id": query["id"], "request": payload,
                             "verdict": "failed", "transport_error": str(error)})
            continue
        # Retain the original actual body, even when normalization/oracles fail.
        item = {"query_id": query["id"], "request": payload, "response": response}
        try:
            normalized = normalize_search(query, response)
            item["score"] = oracle.retrieval(query, normalized, fixtures.corpus())
            item["verdict"] = "passed"
        except (oracle.Rejected, KeyError, TypeError, ValueError) as error:
            item.update(verdict="failed", reason=str(error))
        captures.append(item)
    return {"format": "recall-preview-capture/v1", "source_sha": args.source_sha,
            "artifact_sha256": args.artifact_sha256, "backend": args.backend,
            "provider_profile": args.provider_profile, "fixture_sha256": fixtures.corpus_sha256(),
            "transport": "http", "space": args.space, "captures": captures,
            "scope": "public-search-only; does not qualify all gates or consumer adoption"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("inventory", help="list all unexecuted gates; never probes")
    generated = commands.add_parser("fixtures", help="emit synthetic source inputs; never ingests")
    generated.add_argument("--space", required=True)
    generated.add_argument("--count", type=int, default=50)
    evaluate = commands.add_parser("evaluate", help="validate externally captured observations")
    evaluate.add_argument("oracle", choices=sorted(oracle.CHECKS))
    evaluate.add_argument("observation", type=Path)
    evaluate.add_argument("--workload-contract", type=Path,
                          help="independently agreed pre-run contract, required for workload evaluation")
    probe = commands.add_parser("capture-http", help="bounded, original-query capture against an already installed candidate")
    for name in ("url", "space", "source-sha", "artifact-sha256", "backend", "provider-profile", "token-env"):
        probe.add_argument("--" + name, required=True)
    probe.add_argument("--timeout", type=float, required=True)
    probe.add_argument("--max-bytes", type=int, required=True)
    probe.add_argument("--dispatch-governed-provider", action="store_true")
    probe.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "inventory":
            value = {"gates": matrix.inventory(), "backends": matrix.BACKENDS, "artifacts": matrix.ARTIFACTS}
        elif args.command == "fixtures":
            value = {"fixture_sha256": fixtures.corpus_sha256(), "sources": fixtures.structured_sources(args.space, args.count)}
        elif args.command == "evaluate":
            contract = (strict_json_body(args.workload_contract.read_bytes())
                        if args.workload_contract else None)
            value = oracle.evaluate(args.oracle, strict_json_body(args.observation.read_bytes()),
                                    workload_contract=contract)
        else:
            oracle.require(not args.output.exists(), "capture output already exists")
            value = capture(args)
            # New evidence only; never silently overwrite a prior receipt.
            descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(value, stream, indent=2, ensure_ascii=False)
                stream.write("\n")
        print(json.dumps(value, indent=2, ensure_ascii=False))
        failed = value.get("verdict") == "failed" or any(row["verdict"] == "failed" for row in value.get("captures", []))
        return 1 if failed else 0
    except (oracle.Rejected, OSError, ValueError, KeyError, TypeError) as error:
        print(json.dumps({"verdict": "unmet-prerequisite", "reason": str(error)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
