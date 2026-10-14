"""Explicit inactive-by-default authority for the separate D28 allocation."""
from __future__ import annotations

import ipaddress
from pathlib import Path
import secrets
import time

from cohort import POLICY, SPACES, allocation_contract
from cohort_profile import D28, SEMANTIC140
from ledger import BACKENDS, RESPONSE_CAP
from policy import private_read
from wire import BOUND_VERSION, Scope, digest, encode, fields, require, strict_json, text

FORMAT = "recall-exclusive-embedding-manifest/v1"
CONTROL_PATH = "/v1/_qualification/cohort"
ALLOCATION_SHA256 = "0a54fcd4c6359e39611567a06857f2d61975b3f19a80052f6ea49691f6d1633f"
ALLOCATION_CONTENT_SHA256 = "4abbb62aba6656d134f183841116fb0af2a09752e899a3088748a5b4f51c966a"


def hash_value(value) -> str:
    value = text(value)
    require(len(value) == 64 and value != "0" * 64 and all(c in "0123456789abcdef" for c in value), "missing_pinned_digest")
    return value


TYPED_PINS = {
    "preparation_contract": "b1fd4c43e1c8c506ca13e261a29d65a7d06246bf11ef53c02e97945cd3ef56d1",
    "typed_delta": "51b34aec951d783a93b903a1468fba9a8833ba2f31f705bd784c66c9b26688f8",
}


class CohortPolicy:
    settlement = False
    cohort = True
    output_cap_mode = "reject"
    context_policy = BOUND_VERSION
    profile = D28
    connection_slots = 64

    def __init__(self, manifest: Path, approval: Path):
        self.manifest_path, self.approval_path = manifest, approval
        raw = private_read(manifest)
        self.manifest_hash = digest(raw)
        m = self.manifest = strict_json(raw)
        required = {"format", "live_ready", "run_id", "run_directory", "accounting_policy", "account_id",
                    "credential_generation", "artifact_sha256", "configuration_sha256", "runtime_sha256",
                    "gateway_sha256", "contract_sha256", "corpus_sha256", "allocation_path", "allocation_sha256",
                    "bindings", "endpoint", "gateway_endpoint", "upstream_ips", "synthetic_corpus_only",
                    "transport_response_byte_cap", "bound_version"}
        if self.profile == SEMANTIC140:
            required.add("profile_id")
        required |= {name + suffix for name in self.profile.typed_pins for suffix in ("_path", "_sha256")}
        fields(m, required, required)
        require(m["format"] == self.profile.manifest_format and m["live_ready"] is True, "live_not_ready")
        if self.profile == SEMANTIC140:
            require(m["profile_id"] == self.profile.profile_id, "cohort_profile_mismatch")
        require(m["accounting_policy"] == self.profile.accounting_policy and m["bound_version"] == BOUND_VERSION, "cohort_policy_mismatch")
        allocation_bytes = private_read(Path(m["allocation_path"]))
        self.allocation = strict_json(allocation_bytes)
        require(digest(allocation_bytes) == m["allocation_sha256"] == self.profile.allocation_sha256
                and digest(encode(self.allocation)) == self.profile.allocation_content_sha256, "cohort_allocation_mismatch")
        require(m["contract_sha256"] == self.allocation["custody"]["contract_sha256"]
                and m["corpus_sha256"] == self.allocation["custody"]["corpus_sha256"], "cohort_corpus_mismatch")
        if self.profile == SEMANTIC140:
            require(self.allocation.get("profile_id") == self.profile.profile_id
                    and m["contract_sha256"] == self.profile.contract_sha256
                    and m["corpus_sha256"] == self.profile.corpus_sha256, "cohort_profile_mismatch")
        for name, expected in self.profile.typed_pins.items():
            require(digest(private_read(Path(m[name + "_path"]))) == m[name + "_sha256"] == expected,
                    "cohort_typed_preparation_mismatch")
        require(m["endpoint"] == "https://api.openai.com/v1"
                and m["gateway_endpoint"] == "http://127.0.0.1:43129/v1", "endpoint_mismatch")
        require(m["synthetic_corpus_only"] is True and m["transport_response_byte_cap"] == RESPONSE_CAP, "capture_scope_mismatch")
        for key in required:
            if key.endswith("_sha256"):
                hash_value(m[key])
        require(type(m["credential_generation"]) is int and 0 < m["credential_generation"] <= 2**63 - 1,
                "cohort_credential_generation")
        for key in ("run_id", "account_id"):
            require(0 < len(text(m[key])) <= 128, "cohort_identity_length")
        require(type(m["upstream_ips"]) is list and 1 <= len(m["upstream_ips"]) <= 8, "missing_approved_upstream_addresses")
        for value in m["upstream_ips"]:
            address = ipaddress.ip_address(text(value))
            require(address.is_global and not address.is_multicast, "nonglobal_upstream_address")
        require(type(m["bindings"]) is list and len(m["bindings"]) == 4, "cohort_scope_coverage")
        self.scopes = {}
        for binding in m["bindings"]:
            names = {"id", "backend", "space", "operation", "model", "dimensions", "connection_id", "profile_id"}
            fields(binding, names, names)
            for key in ("id", "connection_id", "profile_id"):
                value = text(binding[key])
                require(0 < len(value) <= 128, "cohort_binding_identity")
            name = binding["id"]
            require(all(c.isascii() and (c.isalnum() or c == "_") for c in name)
                    and name not in self.scopes, "invalid_binding_id")
            scope = Scope(binding["backend"], binding["space"], binding["operation"], binding["model"],
                          binding["dimensions"], name, binding["connection_id"], binding["profile_id"], m["credential_generation"])
            require(scope.backend in BACKENDS and scope.space in SPACES and scope.operation == "embedding", "cohort_binding_scope")
            expected = ("text-embedding-3-small", 1536) if scope.space == SPACES[0] else ("text-embedding-3-large", 3072)
            require((scope.model, scope.dimensions) == expected, "cohort_embedding_profile")
            self.scopes[name] = scope
        require(len({(s.backend, s.space) for s in self.scopes.values()}) == 4, "cohort_scope_coverage")
        self.identity = {key: m[key] for key in ("account_id", "credential_generation", "configuration_sha256",
                                              "allocation_sha256", "contract_sha256", "corpus_sha256",
                                              "preparation_contract_sha256", "typed_delta_sha256")}
        self.identity["bindings_sha256"] = digest(encode(m["bindings"]))
        self.approval_hash = digest(private_read(approval))
        self.check()
        self.tokens = {name: secrets.token_urlsafe(32) for name in self.scopes}
        self.capabilities = {digest(token.encode()): self.scopes[name] for name, token in self.tokens.items()}
        self.control_token = secrets.token_urlsafe(32)

    def check(self):
        require(digest(private_read(self.manifest_path)) == self.manifest_hash, "manifest_changed")
        require(digest(private_read(Path(self.manifest["allocation_path"]))) == self.profile.allocation_sha256, "cohort_allocation_changed")
        for name, expected in self.profile.typed_pins.items():
            require(digest(private_read(Path(self.manifest[name + "_path"]))) == expected,
                    "cohort_typed_preparation_changed")
        raw = private_read(self.approval_path)
        require(digest(raw) == self.approval_hash, "approval_changed_or_revoked")
        approval = strict_json(raw)
        reviews = {"bound_review_sha256", "isolation_review_sha256", "binding_review_sha256",
                   "capture_scope_review_sha256", "cohort_review_sha256", "exclusive_run_review_sha256"}
        names = reviews | {"format", "manifest_sha256", "authorized", "expires_unix", "user_approval_id"}
        fields(approval, names, names)
        require(approval["format"] == "recall-exclusive-embedding-approval/v1"
                and approval["authorized"] is True and approval["manifest_sha256"] == self.manifest_hash,
                "approval_required")
        require(type(approval["expires_unix"]) is int and time.time() < approval["expires_unix"], "approval_expired")
        require(bool(text(approval["user_approval_id"])), "spending_approval_required")
        for key in reviews:
            hash_value(approval[key])

    def authenticate(self, authorization):
        require(authorization.startswith("Bearer ") and len(authorization) <= 256, "invalid_capability")
        scope = self.capabilities.get(digest(authorization[7:].encode()))
        require(scope is not None, "invalid_capability")
        return scope

    def authenticate_control(self, authorization):
        require(secrets.compare_digest(authorization, "Bearer " + self.control_token), "invalid_cohort_capability")

    def worker_environment(self):
        env = {f"RECALL_GATEWAY_{name}": token for name, token in self.tokens.items()}
        env.update({"RECALL_GATEWAY_COHORT_CONTROL_TOKEN": self.control_token,
                    "RECALL_GATEWAY_MANIFEST_SHA256": self.manifest_hash,
                    "RECALL_GATEWAY_MAX_CONNECTIONS": str(self.profile.connection_slots),
                    "RECALL_GATEWAY_TIME_CAP": str(self.profile.time_cap)})
        return env

    def control(self, payload, ledger):
        names = {"action", "gateway_manifest_sha256", "allocation_sha256", "contract_sha256", "corpus_sha256",
                 "backend", "phase", "caller_sha256", "binary_sha256", "observation_sha256",
                 "preparation_contract_sha256", "typed_delta_sha256"}
        if self.profile == SEMANTIC140:
            names.add("profile_id")
        required = names - {"observation_sha256"}
        fields(payload, names, required)
        if self.profile == SEMANTIC140:
            require(payload["profile_id"] == self.profile.profile_id, "cohort_profile_mismatch")
        require(payload["gateway_manifest_sha256"] == self.manifest_hash
                and payload["binary_sha256"] == self.manifest["artifact_sha256"], "cohort_manifest_mismatch")
        for key in ("allocation_sha256", "contract_sha256", "corpus_sha256", "preparation_contract_sha256", "typed_delta_sha256"):
            require(payload[key] == self.identity[key], "cohort_contract_mismatch")
        caller = hash_value(payload["caller_sha256"])
        backend, phase = payload["backend"], payload["phase"]
        if payload["action"] == "begin":
            require("observation_sha256" not in payload, "cohort_begin_fields")
            return ledger.begin(backend, phase, caller, self.check)
        require(payload["action"] in ("close", "close_with_evidence"), "cohort_control_action")
        # Closure must remain available after approval expiry/revocation so
        # original reservations and partial captures cannot vanish.
        closer = ledger.close_with_evidence if payload["action"] == "close_with_evidence" else ledger.close_cohort
        return closer(backend, phase, caller, hash_value(payload.get("observation_sha256")))


class Semantic140Policy(CohortPolicy):
    """A separate exact manifest/allocation, retaining original cohort mechanics."""
    profile = SEMANTIC140
    connection_slots = SEMANTIC140.connection_slots
