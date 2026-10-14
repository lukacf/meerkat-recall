#!/usr/bin/env python3
"""Offline evaluation only. Does not start Recall or perform network/model calls."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tests/recall_preview_semantics"))
from controls import DIMENSIONS, SEEDS, negative_control
from oracle import CORPUS, digest, evaluate, load_corpus


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, default=CORPUS)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("inventory", help="Authored fixture inventory; never an acceptance result")
    sub.add_parser("fake-vectors", help="Run offline seeded numeric negative controls only")
    scoring = sub.add_parser("evaluate", help="Score exact captured public responses and independent D review")
    scoring.add_argument("capture", type=Path)
    scoring.add_argument("--review", type=Path, required=True)
    args = parser.parse_args()
    try:
        corpus = load_corpus(args.corpus)
        if args.command == "inventory":
            result = {"version": corpus["version"], "corpus_sha256": digest(corpus),
                      "queries": len(corpus["queries"]), "source_revisions": len(corpus["sources"]),
                      "counterexamples": len(corpus["unsupported_claims"]),
                      "verdict": "authored-unexecuted", "thresholds": corpus["metric"]}
            success = True
        elif args.command == "fake-vectors":
            runs = [negative_control(corpus, seed, dims) for seed in SEEDS for dims in DIMENSIONS]
            result = {"corpus_sha256": digest(corpus), "runs": runs, "product_acceptance": False}
            success = all(run["negative_control_pass"] for run in runs)
        else:
            capture = json.loads(args.capture.read_text())
            review = json.loads(args.review.read_text())
            result = evaluate(corpus, capture, review)
            success = result["verdict"] == "pass"
        print(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False))
        return 0 if success else 1
    except (AssertionError, KeyError, TypeError, ValueError, OSError) as error:
        print(json.dumps({"verdict": "invalid-evidence", "error": str(error)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
