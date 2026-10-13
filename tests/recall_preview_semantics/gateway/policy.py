"""Trusted host configuration. The checked-in proposal cannot start a live run."""
from __future__ import annotations

import ipaddress
import os
from pathlib import Path
import secrets
import stat
import time

from ledger import BACKENDS, OPERATIONS, RESPONSE_CAP
from wire import BOUND_VERSION, Denied, Scope, digest, encode, fields, require, strict_json, text


def private_read(path: Path) -> bytes:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid()
                and not info.st_mode & 0o077 and info.st_size <= 131072, "unsafe_private_file")
        return os.read(fd, 131073)
    finally:
        os.close(fd)


class Policy:
    def __init__(self, manifest: Path, approval: Path):
        self.manifest_path = manifest
        self.approval_path = approval
        raw = private_read(manifest)
        self.manifest_hash = digest(raw)
        self.manifest = strict_json(raw)
        required = {"format", "run_id", "proposal_sha256", "catalog_sha256", "corpus_sha256",
                    "artifact_sha256", "configuration_sha256", "account_id", "upstream_ips",
                    "bindings", "endpoint", "gateway_endpoint", "bound_version", "live_ready",
                    "runtime_sha256", "transport_response_byte_cap", "synthetic_corpus_only", "gateway_sha256", "run_directory"}
        self.settlement = self.manifest.get("format") == "recall-gateway-manifest/v2"
        if self.settlement:
            required |= {"accounting_policy", "rate_version", "credential_generation", "reasoning_sdk_sha256", "reasoning_projection_version"}
        fields(self.manifest, required | {"output_cap_mode"}, required)
        m = self.manifest
        self.output_cap_mode = m.get("output_cap_mode", "preserve" if self.settlement else "reject")
        require(self.output_cap_mode in (("preserve", "cap_at_16384") if self.settlement else
                                         ("reject", "cap_at_16384")), "unapproved_transform_mode")
        require(m["format"] in ("recall-gateway-manifest/v1", "recall-gateway-manifest/v2")
                and m["live_ready"] is True, "live_not_ready")
        require(m["endpoint"] == "https://api.openai.com/v1" and m["gateway_endpoint"] == "http://127.0.0.1:43129/v1", "endpoint_mismatch")
        self.context_policy = m["bound_version"]
        if self.settlement:
            from terminal_usage import POLICY, RATE_VERSION, BOUND_VERSION as FULL_BOUND, SDK_SOURCE_SHA256, SDK_PROJECTION_VERSION
            require(m["accounting_policy"] == POLICY and m["rate_version"] == RATE_VERSION
                    and self.context_policy == FULL_BOUND, "settlement_policy_mismatch")
            require(m["reasoning_sdk_sha256"] == SDK_SOURCE_SHA256
                    and m["reasoning_projection_version"] == SDK_PROJECTION_VERSION, "settlement_sdk_projection_mismatch")
            require(type(m["credential_generation"]) is int and 0 < m["credential_generation"] <= 2**63-1,
                    "settlement_credential_generation")
        else:
            require(self.context_policy == BOUND_VERSION, "bound_version_mismatch")
        require(m["transport_response_byte_cap"] == RESPONSE_CAP and m["synthetic_corpus_only"] is True,
                "capture_scope_mismatch")
        for name in ("proposal_sha256", "catalog_sha256", "corpus_sha256", "artifact_sha256", "configuration_sha256", "runtime_sha256", "gateway_sha256"):
            value = text(m[name])
            require(len(value) == 64 and all(c in "0123456789abcdef" for c in value), "missing_pinned_digest")
        require(m["catalog_sha256"] == "9b40bf9b99a93b3f6d1ba7cf0b402fd46b01b4aac42205c9681c8e894a03e2ce"
                and m["corpus_sha256"] == "fa6a84149483d3511fd559ac9a13e89d6c1c041ae97d6f15084b52b7d7f21c1b", "frozen_proposal_mismatch")
        require(bool(text(m["run_id"])) and bool(text(m["account_id"])), "missing_run_identity")
        require(type(m["upstream_ips"]) is list and 1 <= len(m["upstream_ips"]) <= 8, "missing_approved_upstream_addresses")
        for address in m["upstream_ips"]:
            try:
                parsed = ipaddress.ip_address(text(address))
            except ValueError as error:
                raise Denied("invalid_upstream_address") from error
            require(parsed.is_global and not parsed.is_multicast, "nonglobal_upstream_address")
        require(type(m["bindings"]) is list and 1 <= len(m["bindings"]) <= 64, "missing_bindings")
        self.scopes = {}
        for binding in m["bindings"]:
            fields(binding, {"id", "backend", "space", "operation", "model", "dimensions", "connection_id", "profile_id"},
                   {"id", "backend", "space", "operation", "model", "connection_id", "profile_id"})
            name = text(binding["id"])
            require(name and all(c.isascii() and (c.isalnum() or c == "_") for c in name)
                    and name not in self.scopes, "invalid_binding_id")
            scope = Scope(binding["backend"], binding["space"], binding["operation"], binding["model"], binding.get("dimensions"),
                          binding_id=name if self.settlement else "",
                          connection_id=binding["connection_id"] if self.settlement else "",
                          profile_id=binding["profile_id"] if self.settlement else "",
                          credential_generation=m["credential_generation"] if self.settlement else 0)
            require(scope.backend in BACKENDS and scope.operation in OPERATIONS
                    and scope.space in ("semantics-household", "semantics-business"), "invalid_binding_scope")
            require(bool(text(binding["connection_id"])) and bool(text(binding["profile_id"])), "missing_binding_identity")
            if scope.operation == "embedding":
                model, dimension = ("text-embedding-3-small", 1536) if scope.space == "semantics-household" else ("text-embedding-3-large", 3072)
                require(scope.model == model and scope.dimensions == dimension, "embedding_binding_mismatch")
            else:
                require(scope.model == "gpt-6.1-sol" and scope.dimensions is None, "generation_binding_mismatch")
            self.scopes[name] = scope
        # A connection/profile cannot carry two operation capabilities. Enforcing
        # this requires a reviewed per-role Recall configuration, currently absent.
        connections = [(b["backend"], b["space"], b["connection_id"]) for b in m["bindings"]]
        profiles = [(b["backend"], b["space"], b["profile_id"]) for b in m["bindings"]]
        require(len(set(connections)) == len(connections) and len(set(profiles)) == len(profiles), "shared_role_binding")
        self.approval_hash = digest(private_read(approval))
        self.check()
        self.tokens = {name: secrets.token_urlsafe(32) for name in self.scopes}
        self.capabilities = {digest(token.encode()): self.scopes[name] for name, token in self.tokens.items()}

    def check(self) -> None:
        """Re-read trusted approval at each reservation and actual send boundary."""
        require(digest(private_read(self.manifest_path)) == self.manifest_hash, "manifest_changed")
        raw = private_read(self.approval_path)
        require(digest(raw) == self.approval_hash, "approval_changed_or_revoked")
        approval = strict_json(raw)
        required = {"format", "manifest_sha256", "authorized", "expires_unix", "user_approval_id",
                    "bound_review_sha256", "isolation_review_sha256", "binding_review_sha256",
                    "capture_scope_review_sha256"}
        if self.output_cap_mode == "cap_at_16384":
            required.add("output_transform_review_sha256")
        if self.settlement:
            required |= {"settlement_review_sha256", "context_provenance_review_sha256"}
        fields(approval, required, required)
        require(approval["format"] == "recall-gateway-approval/v1" and approval["authorized"] is True
                and approval["manifest_sha256"] == self.manifest_hash, "approval_required")
        require(type(approval["expires_unix"]) is int and time.time() < approval["expires_unix"], "approval_expired")
        require(bool(text(approval["user_approval_id"])), "spending_approval_required")
        for key in required - {"format", "manifest_sha256", "authorized", "expires_unix", "user_approval_id"}:
            value = text(approval[key])
            require(len(value) == 64 and all(c in "0123456789abcdef" for c in value) and value != "0" * 64,
                    "independent_review_required")

    def authenticate(self, authorization: str) -> Scope:
        require(authorization.startswith("Bearer ") and len(authorization) <= 256, "invalid_capability")
        scope = self.capabilities.get(digest(authorization[7:].encode()))
        require(scope is not None, "invalid_capability")
        return scope

    def worker_environment(self) -> dict[str, str]:
        # Only newly minted proxy capabilities. No ambient environment is copied.
        return {f"RECALL_GATEWAY_{name}": token for name, token in self.tokens.items()}
