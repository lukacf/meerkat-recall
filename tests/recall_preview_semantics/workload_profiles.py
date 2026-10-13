"""Closed qualification profiles; selecting one never authorizes execution.

The historical D28 documents retain their original bytes. The successor binds
separate immutable inputs, rather than accepting caller-provided quota values.
Independent response/evidence validators own their own expected predicates.
"""
from __future__ import annotations

import hashlib
import json
from types import MappingProxyType
from typing import NamedTuple

SELF = "tests/recall_preview_semantics/workload_profiles.py"
LEGACY_ID = "d28-v1"
SEMANTIC140_ID = "semantic140-v1"


class Profile(NamedTuple):
    id: str
    callers: int
    requests: int
    caller_width: int
    measurement_attempts: int
    total_attempts: int
    money: int
    row_capture_bytes: int
    observation_bytes: int
    runtime_bytes: int
    hashes: object
    contract_canonical_sha256: str | None

    def caller_ids(self):
        return tuple(f"caller-{index:0{self.caller_width}}" for index in range(self.callers))

    def fixed(self):
        return {"callers": self.callers, "requests_per_caller": 4,
                "request_count": self.requests, "source_count": 50,
                "request_timeout_ms": 8000, "duration_budget_ms": 120000,
                "response_budget_bytes": 262144, "p95_ms": 2000, "p99_ms": 5000,
                "wire_limit": 50}

    @property
    def preparation_filename(self):
        return ("semantic140-" if self.id == SEMANTIC140_ID else "workload-") + "preparation-contract.json"


LEGACY = Profile(LEGACY_ID, 50, 200, 2, 600, 2100, 3_000_000_000,
    128 * 1024**2, 64 * 1024**2, 352 * 1024**2, MappingProxyType({
        "corpus": "2192437954109fc17ebbea5b3960eb6b398a2ee91a725ce70666b119e86179dc",
        "contract": "696e0fa0f8736306d6e66f5ce26658a21fb013255e5727b5287406008c350a5a",
        "preparation_contract": "b1fd4c43e1c8c506ca13e261a29d65a7d06246bf11ef53c02e97945cd3ef56d1",
        "rendered_map": "9dbcade15daa3fb504838c2fe4f743b4b5fc37da20b2c0f29e2e3aea49ad57ab",
        "provider_allocation": "0a54fcd4c6359e39611567a06857f2d61975b3f19a80052f6ea49691f6d1633f",
        "typed_delta": "51b34aec951d783a93b903a1468fba9a8833ba2f31f705bd784c66c9b26688f8",
    }), None)

SEMANTIC140 = Profile(SEMANTIC140_ID, 140, 560, 3, 1680, 4260, 5_000_000_000,
    512 * 1024**2, 512 * 1024**2, 2 * 1024**3, MappingProxyType({
        "corpus": "e3201eb81bc294391e445a431a847f88603ec0631e23b6068d6f44cf0944ff0f",
        "contract": "d334f2c3fcddd134f4765704bbee91e0d917bab9865fc7c27ff19df7942adaa8",
        "preparation_contract": "6129328500aa899d4995c9ff078571176af1eeb8764b78681531581418e0bc4c",
        "rendered_map": "dff61fd6ecb1cd6a8916df4f1cee7918ad1a106fc7ef5df4a564f8312b9e26fc",
        "provider_allocation": "e42df2638349fea37f94eb7f9ea79d2afad170a2d36a655869a40f25f83dcd01",
        "typed_delta": "e1e9bf38df83d8afdf881924e55847aaffa6174b6ad01ea8e49ebc2a4ff43a5d",
    }), "6518d9f3d6b46afc28c7a87170de567a81e31f3d754966b5e18c08fe590644a2")


def profile(identifier=LEGACY_ID):
    if type(identifier) is str and identifier == LEGACY_ID:
        return LEGACY
    if type(identifier) is str and identifier == SEMANTIC140_ID:
        return SEMANTIC140
    raise ValueError("unknown closed workload profile")


def profile_for_allocation(checksum):
    for selected in (LEGACY, SEMANTIC140):
        if type(checksum) is str and checksum == selected.hashes["provider_allocation"]:
            return selected
    raise ValueError("unknown closed workload allocation")


def profile_for_contract(contract):
    if type(contract) is not dict:
        raise ValueError("invalid workload contract")
    selected = profile(contract.get("profile_id", LEGACY_ID))
    if selected is SEMANTIC140:
        raw = json.dumps(contract, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False).encode()
        if hashlib.sha256(raw).hexdigest() != selected.contract_canonical_sha256:
            raise ValueError("changed closed semantic140 contract")
    elif contract.get("corpus_sha256") == SEMANTIC140.hashes["corpus"]:
        raise ValueError("successor corpus under historical profile")
    return selected


def profile_for_manifest(manifest):
    if type(manifest) is not dict:
        raise ValueError("invalid workload manifest")
    selected = profile(manifest.get("profile_id", LEGACY_ID))
    if selected is SEMANTIC140:
        if any(manifest.get(name + "_sha256") != value for name, value in selected.hashes.items()):
            raise ValueError("semantic140 input/profile mismatch")
    elif any(manifest.get(name + "_sha256") == value for name, value in SEMANTIC140.hashes.items()):
        raise ValueError("successor input under historical profile")
    return selected


def profile_for_review(review):
    try:
        checksum = review["controller_policy"]["expected_anchor"]["allocation_sha256"]
    except (KeyError, TypeError) as error:
        raise ValueError("workload review allocation absent") from error
    selected = profile_for_allocation(checksum)
    if profile(review.get("profile_id", LEGACY_ID)) is not selected:
        raise ValueError("workload review/profile mismatch")
    return selected


def validate_corpus(corpus, selected):
    if selected is SEMANTIC140:
        raw = json.dumps(corpus, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False).encode()
        if hashlib.sha256(raw).hexdigest() != "90d8c824efb134810d5e37a0b8543f2a981322848ad546c9b68c22dbe92b8faa":
            raise ValueError("changed closed semantic140 corpus")
