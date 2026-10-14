"""Deterministic synthetic inputs; generation is not an ingestion or load run."""

import hashlib
import json
from pathlib import Path


CORPUS_PATH = Path(__file__).with_name("corpus.json")


def corpus() -> dict:
    return json.loads(CORPUS_PATH.read_text(encoding="utf-8"))


def corpus_sha256() -> str:
    return hashlib.sha256(CORPUS_PATH.read_bytes()).hexdigest()


def sized_text(size: int, multibyte: bool = False) -> str:
    if size < 0:
        raise ValueError("size must be nonnegative")
    if multibyte:
        return "é" * (size // 2) + "x" * (size % 2)
    return "x" * size


def structured_sources(space: str, count: int = 50) -> list[dict]:
    if space not in {entry["id"] for entry in corpus()["spaces"]}:
        raise ValueError("explicit known named space required")
    if count < 1:
        raise ValueError("count must be positive")
    return [{
        "space": space,
        "key": f"scope-report-{index:03d}",
        "revision": f"authoritative-{index:03d}-v1",
        "text": f"# Review {index:03d}\n\nThe owner of initiative {index:03d} is Reviewer {index:03d}.\n"
                f"Its approved capacity is {17 + index} units.\n\nThis record belongs to {space}.\n",
        "source_url": f"https://example.invalid/reviews/{index:03d}",
        "labels": {"scope": space, "initiative_id": f"{index:03d}"},
        "kind": "authoritative",
    } for index in range(count)]


def source_request(source: dict, expected_generation: str | None = None) -> dict:
    """Wire draft 1 from surface; observation time intentionally not source order."""
    if source.get("kind") == "degraded-withheld":
        raise ValueError("consumer must withhold degraded renderings")
    return {
        "revision": source["revision"], "text": source["text"], "media_type": "text/plain",
        "source_url": source.get("source_url", "https://example.invalid/qualification"),
        "observed_at": "2026-10-06T20:11:00Z", "labels": source.get("labels", {}),
        "expected_generation": expected_generation,
    }
