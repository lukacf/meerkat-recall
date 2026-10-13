#!/usr/bin/env python3
"""Check the frozen, offline provider proposal and conservative budget arithmetic.

This tool has no network, credential, inference or dispatch function. Its output
is never a readiness, spending-authorization or product-acceptance receipt.
"""

from __future__ import annotations

import argparse
from decimal import Decimal
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT = ROOT / "tests/recall_preview/primary-provider-proposal.json"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def positive_integer(value: object, name: str, *, zero: bool = False) -> int:
    require(type(value) is int and value >= (0 if zero else 1), f"invalid {name}")
    return value


def money(value: object, name: str) -> Decimal:
    require(isinstance(value, str), f"{name} must be an exact decimal string")
    result = Decimal(value)
    require(result.is_finite() and result >= 0, f"invalid {name}")
    return result


def review(plan: dict, catalog: dict, seal: dict) -> dict:
    require(plan["format"] == "recall-primary-provider-proposal/v1", "unknown proposal format")
    require(plan["status"] == "proposal-not-authorized"
            and plan["evidence_class"] == "planning-only"
            and plan["execution_authorized"] is False, "proposal cannot authorize execution")
    require(plan["corpus_canonical_sha256"] == seal["corpus_canonical_sha256"], "corpus seal changed")
    require(plan["source_revisions_per_backend"] == seal["source_revisions"]
            and plan["original_queries_per_backend"] == seal["original_queries"], "corpus cases removed")
    require(plan["backends"] == ["surreal-db", "elephant-db"], "both retained backends required")
    require(plan["ranking_modes"] == ["semantic", "hybrid"], "separate ranking modes required")
    provider = plan["provider"]
    require(provider["endpoint"] == "https://api.openai.com/v1", "unreviewed provider endpoint")
    require(provider["version_kind"] == "operator-pinned-configuration-with-mutable-provider-alias",
            "catalog cannot establish immutable remote weights")
    require(provider["account_isolation_claim"] is False, "one account cannot establish account isolation")
    require(provider["service_tier"] == "standard" and provider["regional_processing"] is False
            and provider["hosted_paid_tools"] is False, "pricing scope changed")
    spaces = plan["spaces"]
    require([(s["id"], s["embedding_model"], s["dimensions"]) for s in spaces] == [
        ("semantics-household", "text-embedding-3-small", 1536),
        ("semantics-business", "text-embedding-3-large", 3072)], "unreviewed space/model/dimension change")
    for field in ("connection_id", "embedding_profile_id", "generation_profile_id", "policy"):
        require(len({s[field] for s in spaces}) == 2, f"spaces share {field}")
    generation = plan["generation"]
    require(generation["provider"] == "openai" and generation["model"] == "gpt-6.1-sol"
            and generation["reasoning_effort"] == "medium", "unreviewed generation identity")
    model = catalog["models"][generation["model"]]
    require(model["identity"] == "mutable-provider-alias", "catalog identity altered")
    input_cap = positive_integer(generation["max_input_tokens"], "generation input cap")
    output_cap = positive_integer(generation["max_output_tokens"], "generation output cap")
    require(input_cap <= 272000, "long-context premium not reserved")
    require(output_cap <= model["max_output_tokens"]
            and input_cap + output_cap <= model["context_tokens"], "model token limit exceeded")
    limits = plan["dispatch_limits_per_backend"]
    expected_roles = {"document_embedding", "query_embedding", "triage", "extraction",
                      "support_verification", "summary", "promotion_and_identity_disambiguation",
                      "truth_maintenance", "wiki_planner_writer_verifier"}
    require(set(limits) == expected_roles, "role budget missing or unknown")
    for role, count in limits.items():
        positive_integer(count, role, zero=True)
    require(limits["triage"] == 0, "deterministic triage is not a model call")
    require(limits["document_embedding"] >= plan["source_revisions_per_backend"]
            and limits["query_embedding"] >= 2 * plan["original_queries_per_backend"],
            "embedding allowance cannot cover frozen inputs")
    backends = len(plan["backends"])
    embedding_calls = backends * (limits["document_embedding"] + limits["query_embedding"])
    calls = backends * sum(limits.values())
    generation_calls = calls - embedding_calls
    budget = plan["budgets"]
    require(calls == budget["dispatches_both_backends"], "dispatch ceiling arithmetic mismatch")
    for field in ("wall_time_seconds", "max_concurrent_dispatches", "model_deadline_seconds", "response_bytes"):
        positive_integer(budget[field], field)
    rates = model["usd_per_million"]
    input_rate = max(money(rates["input"], "input price"), money(rates["cache_write"], "cache-write price"))
    generation_reservation = (input_cap * input_rate + output_cap * money(rates["output"], "output price")) / 1_000_000
    embedding_cap = positive_integer(plan["embedding_max_total_input_tokens_per_dispatch"], "embedding batch cap")
    embedding_rates = []
    for space in spaces:
        embedding = catalog["models"][space["embedding_model"]]
        require(embedding_cap <= embedding["max_input_tokens_per_item"], "embedding input cap exceeds model")
        require(space["dimensions"] == embedding["default_dimensions"], "embedding dimensions changed")
        embedding_rates.append(money(embedding["usd_per_million"]["input"], "embedding price"))
    embedding_reservation = embedding_cap * max(embedding_rates) / 1_000_000
    total = generation_calls * generation_reservation + embedding_calls * embedding_reservation
    require(total <= money(budget["usd_stop_cap"], "USD cap"), "worst-case reservations exceed proposed USD cap")
    require(bool(plan["unfilled_prerequisites"]), "planning tool cannot clear actual readiness prerequisites")
    return {
        "evidence_class": "offline-proposal-arithmetic-only", "execution_authorized": False,
        "generation_dispatches": generation_calls, "embedding_dispatches": embedding_calls,
        "total_dispatches": calls, "usd_per_generation_reservation": str(generation_reservation),
        "usd_per_embedding_reservation": str(embedding_reservation), "maximum_reserved_usd": str(total),
        "proposed_usd_cap": budget["usd_stop_cap"], "unfilled_prerequisites": plan["unfilled_prerequisites"],
        "limit": "No installed dispatch enforcement, account access, semantic result or approval was tested.",
    }


def load_and_review(path: Path) -> dict:
    plan = json.loads(path.read_text())
    catalog_path = path.with_name(plan["catalog_file"])
    catalog_bytes = catalog_path.read_bytes()
    require(hashlib.sha256(catalog_bytes).hexdigest() == plan["catalog_file_sha256"], "catalog facts digest mismatch")
    seal = json.loads(path.with_name(plan["expectations_file"]).read_text())
    return review(plan, json.loads(catalog_bytes), seal)


def review_gateway_delta(delta: dict, plan: dict, arithmetic: dict) -> dict:
    """Review explicit gateway/configuration deltas, never execution readiness."""
    require(delta["format"] == "recall-gateway-proposal-delta/v1"
            and delta["status"] == "proposal-not-authorized"
            and delta["execution_authorized"] is False, "gateway delta cannot authorize execution")
    require(delta["corpus_canonical_sha256"] == plan["corpus_canonical_sha256"], "gateway changed corpus")
    require(delta["physical_provider_endpoint"] == "http://127.0.0.1:43129/v1"
            and delta["fixed_upstream_endpoint"] == plan["provider"]["endpoint"]
            and delta["direct_endpoint_parity_claim"] is False
            and delta["partition_configuration_records_physical_endpoint"] is True,
            "gateway endpoint provenance changed")
    require(delta["output_cap_transform"] == {
        "strict_default": "reject", "proposed_opt_in_mode": "cap_at_16384",
        "effective_output_token_ceiling": 16384, "actual_extractor_requested_cap": 64000,
        "separate_run_approval_reference_required": True,
        "preserve_original_and_forwarded_bodies": True,
        "record_requested_effective_caps_and_transform": True,
        "repair_malformed_caps": False, "change_product_default": False,
        "relax_quality_when_truncated": False,
    }, "unreviewed output transform or quality relaxation")
    expected = dict(plan["dispatch_limits_per_backend"])
    expected["embedding"] = expected.pop("document_embedding") + expected.pop("query_embedding")
    require(delta["authenticated_quotas_per_backend"] == expected, "gateway quota arithmetic changed")
    for role, count in delta["authenticated_quotas_per_backend"].items():
        positive_integer(count, role, zero=True)
    embedding = delta["embedding_capability"]
    require(embedding["operations"] == ["document_embedding", "query_embedding"]
            and embedding["one_active_profile_per_backend_and_space"] is True
            and embedding["separate_operation_quota_enforcement_claim"] is False,
            "shared embedding capability misrepresented")
    require(embedding["required_completed_dispatches_per_backend"] == {
        role: plan["dispatch_limits_per_backend"][role]
        for role in ("document_embedding", "query_embedding")
    }, "required embedding coverage weakened")
    limits = delta["unchanged_global_limits"]
    require(set(limits) == {"dispatches_both_backends", "usd_stop_cap", "maximum_reserved_usd",
                            "wall_time_seconds", "max_concurrent_dispatches", "model_deadline_seconds"}
            and all(limits[key] == plan["budgets"][key] for key in limits if key != "maximum_reserved_usd")
            and limits["maximum_reserved_usd"] == arithmetic["maximum_reserved_usd"],
            "gateway changed global budget")
    capture = delta["capture"]
    require(capture["synthetic_corpus_only"] is True
            and capture["request_bytes_each"] == 131072
            and capture["upstream_response_bytes_each"] == 8388608
            and capture["request_copies_each"] == 2
            and capture["public_search_response_bytes"] == plan["budgets"]["response_bytes"],
            "unreviewed transport/public capture limit")
    body_bytes = limits["dispatches_both_backends"] * (
        capture["upstream_response_bytes_each"]
        + capture["request_bytes_each"] * capture["request_copies_each"]
    )
    require(capture["maximum_body_bytes_both_backends"] == body_bytes, "capture size arithmetic mismatch")
    require(capture["directories_mode"] == "0700" and capture["files_mode"] == "0600"
            and capture["retain_credential_headers"] is False
            and capture["oversize_is_incomplete"] is True
            and capture["completed_receipt_requires_body_fsync"] is True, "capture custody weakened")
    require(delta["readiness"] == {
        "token_framing_bound_qualified": False,
        "installed_isolation_qualified": False,
        "actual_role_binding_mapping_qualified": False,
        "sdk_continuations_qualified": False,
        "real_semantics_qualified": False,
    }, "offline delta cannot establish live readiness")
    return {"evidence_class": "offline-gateway-proposal-only", "execution_authorized": False,
            "maximum_reserved_usd": arithmetic["maximum_reserved_usd"],
            "maximum_capture_body_bytes": body_bytes,
            "embedding_quota_per_backend": expected["embedding"],
            "limit": "Combined quota and private capture proposal only; provider framing/configuration/installed execution remain unqualified."}


def load_gateway_delta(path: Path) -> dict:
    delta = json.loads(path.read_text())
    base = path.with_name(delta["base_proposal_file"])
    raw = base.read_bytes()
    require(hashlib.sha256(raw).hexdigest() == delta["base_proposal_sha256"], "gateway base proposal digest mismatch")
    return review_gateway_delta(delta, json.loads(raw), load_and_review(base))


def review_full_context(proposal: dict, facts: dict, primary: dict, arithmetic: dict) -> dict:
    """Separate expensive alternatives; never mutate/authorize the v1 packet."""
    require(proposal["format"] == "recall-full-context-alternatives/v1"
            and proposal["status"] == "proposal-not-authorized"
            and proposal["execution_authorized"] is False
            and proposal["selected_option"] is None, "alternative cannot authorize execution")
    require(proposal["corpus_canonical_sha256"] == primary["corpus_canonical_sha256"], "alternative changed corpus")
    require(proposal["model"] == facts["model"] == primary["generation"]["model"]
            and proposal["reasoning_effort"] == primary["generation"]["reasoning_effort"], "alternative changed generation identity")
    require(facts["format"] == "recall-full-context-catalog/v1"
            and facts["service_tier"] == "standard"
            and facts["regional_processing"] is False and facts["hosted_paid_tools"] is False,
            "unreviewed full-context pricing scope")
    require(proposal["generation_dispatches"] == arithmetic["generation_dispatches"]
            and proposal["embedding_dispatches"] == arithmetic["embedding_dispatches"]
            and type(proposal["count_endpoint_dispatches"]) is int
            and proposal["count_endpoint_dispatches"] == 0, "alternative changed physical call allowance")
    full_input = positive_integer(proposal["input_reservation_tokens_per_generation"], "full input reservation")
    require(full_input == positive_integer(facts["context_tokens"], "documented context")
            and proposal["input_admission_basis"] == "provider-documented-full-context-limit",
            "alternative did not reserve the entire documented context")
    require(proposal["refund_policy"] == "never-refund-any-physical-attempt-or-unknown-usage"
            and proposal["retries_consume_original_role_and_global_quotas"] is True,
            "physical retries or unknown usage cannot release reservations")
    budget = primary["budgets"]
    require(proposal["unchanged_limits"] == {
        "physical_dispatches": budget["dispatches_both_backends"],
        "wall_time_seconds": budget["wall_time_seconds"],
        "max_concurrent_dispatches": budget["max_concurrent_dispatches"],
        "model_deadline_seconds": budget["model_deadline_seconds"],
        "embedding_total_input_tokens_per_dispatch": primary["embedding_max_total_input_tokens_per_dispatch"],
        "public_search_response_bytes": budget["response_bytes"],
    }, "alternative changed nonfinancial limits")
    rates = facts["long_context_usd_per_million"]
    require(rates == {"input": "4.00", "cache_write": "5.00", "output": "15.00"},
            "unreviewed long-context rates; re-review changed official pricing")
    input_rate = max(money(rates["input"], "input price"), money(rates["cache_write"], "cache-write price"))
    output_rate = money(rates["output"], "output price")
    expected = {
        "keep-reviewed-16384-transform": (primary["generation"]["max_output_tokens"], "explicit-cap-at-16384"),
        "preserve-requested-output-through-64000": (64000, "preserve-or-reject"),
        "preserve-requested-output-through-model-maximum": (facts["max_output_tokens"], "preserve-or-reject"),
    }
    require(len(proposal["options"]) == len(expected)
            and {option["id"] for option in proposal["options"]} == set(expected), "alternative options changed")
    options = []
    for option in proposal["options"]:
        output = positive_integer(option["output_reservation_tokens"], "output reservation")
        require((output, option["output_mode"]) == expected[option["id"]]
                and output <= facts["max_output_tokens"], "output reservation/transform changed")
        # Deliberately do not subtract output from context: this reserves both
        # maxima independently even when they cannot be reached simultaneously.
        per_generation = (full_input * input_rate + output * output_rate) / 1_000_000
        total = (arithmetic["generation_dispatches"] * per_generation
                 + arithmetic["embedding_dispatches"] * money(arithmetic["usd_per_embedding_reservation"], "embedding reservation"))
        require(total == money(option["maximum_reserved_usd"], "declared reservation"), "alternative arithmetic mismatch")
        require(total <= money(option["proposed_usd_stop_cap"], "proposed cap"), "alternative cap cannot cover full reservations")
        options.append({"id": option["id"], "usd_per_generation_reservation": str(per_generation),
                        "maximum_reserved_usd": str(total), "proposed_usd_stop_cap": option["proposed_usd_stop_cap"]})
    require(proposal["count_option"] == {
        "status": "less-conservative-separate-unpriced-proposal",
        "physical_dispatches_if_one_count_per_generation": arithmetic["total_dispatches"] + arithmetic["generation_dispatches"],
        "count_billing_bound_usd": None, "execution_authorized": False,
    }, "unreviewed count economics or call allowance")
    require(bool(proposal["provider_contract_assumptions"]) and bool(proposal["gateway_requirements"]),
            "contract dependence and gateway prerequisites must remain explicit")
    return {"evidence_class": "offline-full-context-arithmetic-only", "execution_authorized": False,
            "total_dispatches": arithmetic["total_dispatches"], "count_endpoint_dispatches": 0,
            "options": options, "limit": "Provider-contract cost upper estimates only; no installed enforcement, provider call, semantic acceptance or spending approval."}


def load_full_context(path: Path) -> dict:
    proposal = json.loads(path.read_text())
    base = path.with_name(proposal["base_proposal_file"])
    raw = base.read_bytes()
    require(hashlib.sha256(raw).hexdigest() == proposal["base_proposal_sha256"], "full-context base proposal digest mismatch")
    facts = path.with_name(proposal["catalog_file"]).read_bytes()
    require(hashlib.sha256(facts).hexdigest() == proposal["catalog_file_sha256"], "full-context facts digest mismatch")
    return review_full_context(proposal, json.loads(facts), json.loads(raw), load_and_review(base))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", nargs="?", type=Path, default=DEFAULT)
    alternative = parser.add_mutually_exclusive_group()
    alternative.add_argument("--gateway-delta", type=Path)
    alternative.add_argument("--full-context-alternatives", type=Path)
    args = parser.parse_args()
    if args.full_context_alternatives:
        result = load_full_context(args.full_context_alternatives)
    else:
        result = load_gateway_delta(args.gateway_delta) if args.gateway_delta else load_and_review(args.manifest)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
