"""Offline budget/identity counterexamples, never model or product acceptance."""

import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
SCRIPT = HERE.parents[1] / "scripts/qualification/recall_preview_provider_plan.py"
SPEC = importlib.util.spec_from_file_location("provider_plan", SCRIPT)
provider_plan = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(provider_plan)


class ProviderProposalTests(unittest.TestCase):
    def setUp(self):
        self.plan = json.loads((HERE / "primary-provider-proposal.json").read_text())
        self.catalog = json.loads((HERE / "provider-catalog-facts.json").read_text())
        self.seal = json.loads((HERE / "semantic-expectations-approval.json").read_text())

    def review(self):
        return provider_plan.review(self.plan, self.catalog, self.seal)

    def test_conservative_reservation_without_cache_discounts(self):
        result = provider_plan.load_and_review(HERE / "primary-provider-proposal.json")
        self.assertEqual(result["total_dispatches"], 432)
        self.assertEqual(result["generation_dispatches"], 280)
        self.assertEqual(result["embedding_dispatches"], 152)
        self.assertEqual(result["maximum_reserved_usd"], "68.97467392")
        self.assertFalse(result["execution_authorized"])
        self.assertTrue(result["unfilled_prerequisites"])

    def test_previous_twenty_dollar_proposal_cannot_cover_new_limits(self):
        self.plan["budgets"]["usd_stop_cap"] = "20.00"
        with self.assertRaisesRegex(ValueError, "reservations exceed"):
            self.review()

    def test_astra_substitution_cannot_keep_sol_budget(self):
        self.plan["generation"]["model"] = "gpt-6-astra"
        with self.assertRaisesRegex(ValueError, "generation identity"):
            self.review()

    def test_account_and_mutable_alias_limitations_cannot_be_laundered(self):
        for field, value in [("account_isolation_claim", True), ("version_kind", "immutable-weights")]:
            with self.subTest(field=field):
                prior = self.plan["provider"][field]
                self.plan["provider"][field] = value
                with self.assertRaises(ValueError):
                    self.review()
                self.plan["provider"][field] = prior

    def test_dimension_or_space_profile_collision_refused(self):
        original = copy.deepcopy(self.plan)
        for field, value in [("dimensions", 1536), ("embedding_profile_id", self.plan["spaces"][0]["embedding_profile_id"])]:
            with self.subTest(field=field):
                self.plan = copy.deepcopy(original)
                self.plan["spaces"][1][field] = value
                with self.assertRaises(ValueError):
                    self.review()

    def test_case_removal_or_changed_seal_refused(self):
        for field, value in [("source_revisions_per_backend", 27), ("original_queries_per_backend", 23),
                             ("corpus_canonical_sha256", "0" * 64)]:
            with self.subTest(field=field):
                prior = self.plan[field]
                self.plan[field] = value
                with self.assertRaises(ValueError):
                    self.review()
                self.plan[field] = prior

    def test_retries_cannot_be_added_without_reconciling_ceiling(self):
        self.plan["dispatch_limits_per_backend"]["extraction"] += 1
        with self.assertRaisesRegex(ValueError, "ceiling arithmetic"):
            self.review()

    def test_negative_boolean_and_nonfinite_budgets_refused(self):
        for value in [-1, True, 0.5]:
            with self.subTest(value=value):
                self.plan["dispatch_limits_per_backend"]["extraction"] = value
                with self.assertRaisesRegex(ValueError, "invalid extraction"):
                    self.review()
        for value in ["NaN", "Infinity", "-1"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                provider_plan.money(value, "cap")

    def test_unpriced_long_context_fast_tier_or_paid_tool_refused(self):
        original = copy.deepcopy(self.plan)
        for field, value in [("service_tier", "priority"), ("regional_processing", True), ("hosted_paid_tools", True)]:
            with self.subTest(field=field):
                self.plan = copy.deepcopy(original)
                self.plan["provider"][field] = value
                with self.assertRaisesRegex(ValueError, "pricing scope"):
                    self.review()
        self.plan = original
        self.plan["generation"]["max_input_tokens"] = 272001
        with self.assertRaisesRegex(ValueError, "long-context"):
            self.review()

    def test_missing_roles_and_claimed_authorization_refused(self):
        self.plan["execution_authorized"] = True
        with self.assertRaisesRegex(ValueError, "cannot authorize"):
            self.review()
        self.plan["execution_authorized"] = False
        del self.plan["dispatch_limits_per_backend"]["support_verification"]
        with self.assertRaisesRegex(ValueError, "role budget"):
            self.review()

    def test_catalog_byte_change_invalidates_frozen_price_evidence(self):
        with tempfile.TemporaryDirectory(prefix="recall-provider-plan-") as directory:
            target = Path(directory)
            for name in ("primary-provider-proposal.json", "provider-catalog-facts.json", "semantic-expectations-approval.json"):
                (target / name).write_bytes((HERE / name).read_bytes())
            with (target / "provider-catalog-facts.json").open("a") as stream:
                stream.write("\n")
            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                provider_plan.load_and_review(target / "primary-provider-proposal.json")

    def gateway_delta(self):
        return json.loads((HERE / "gateway-proposal-delta.json").read_text())

    def test_gateway_delta_keeps_budget_and_counts_private_body_storage(self):
        result = provider_plan.load_gateway_delta(HERE / "gateway-proposal-delta.json")
        self.assertEqual(result["maximum_reserved_usd"], "68.97467392")
        self.assertEqual(result["embedding_quota_per_backend"], 76)
        self.assertEqual(result["maximum_capture_body_bytes"], 3737124864)
        self.assertFalse(result["execution_authorized"])

    def test_shared_quota_cannot_claim_separate_operation_enforcement_or_drop_coverage(self):
        for field, value in [("separate_operation_quota_enforcement_claim", True),
                             ("required_completed_dispatches_per_backend", {"document_embedding": 27, "query_embedding": 48})]:
            delta = self.gateway_delta()
            delta["embedding_capability"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                provider_plan.review_gateway_delta(delta, self.plan, self.review())

    def test_gateway_cannot_reuse_direct_endpoint_provenance(self):
        for field, value in [("physical_provider_endpoint", "https://api.openai.com/v1"),
                             ("direct_endpoint_parity_claim", True),
                             ("partition_configuration_records_physical_endpoint", False)]:
            delta = self.gateway_delta()
            delta[field] = value
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "endpoint provenance"):
                provider_plan.review_gateway_delta(delta, self.plan, self.review())

    def test_transport_cap_cannot_raise_public_cap_or_claim_complete_truncation(self):
        for field, value in [("upstream_response_bytes_each", 16777216),
                             ("public_search_response_bytes", 8388608),
                             ("oversize_is_incomplete", False),
                             ("retain_credential_headers", True),
                             ("completed_receipt_requires_body_fsync", False)]:
            delta = self.gateway_delta()
            delta["capture"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                provider_plan.review_gateway_delta(delta, self.plan, self.review())

    def test_gateway_offline_pass_cannot_clear_framing_or_installed_readiness(self):
        for field in self.gateway_delta()["readiness"]:
            delta = self.gateway_delta()
            delta["readiness"][field] = True
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "live readiness"):
                provider_plan.review_gateway_delta(delta, self.plan, self.review())

    def test_output_cap_transform_cannot_be_implicit_or_relax_quality(self):
        for field, value in [("strict_default", "cap_at_16384"),
                             ("effective_output_token_ceiling", 64000),
                             ("separate_run_approval_reference_required", False),
                             ("relax_quality_when_truncated", True),
                             ("change_product_default", True)]:
            delta = self.gateway_delta()
            delta["output_cap_transform"][field] = value
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "output transform"):
                provider_plan.review_gateway_delta(delta, self.plan, self.review())


class FullContextAlternativeTests(unittest.TestCase):
    def setUp(self):
        self.proposal = json.loads((HERE / "full-context-provider-alternatives.json").read_text())
        self.facts = json.loads((HERE / "provider-full-context-facts.json").read_text())
        self.primary = json.loads((HERE / "primary-provider-proposal.json").read_text())
        self.arithmetic = provider_plan.load_and_review(HERE / "primary-provider-proposal.json")

    def review(self):
        return provider_plan.review_full_context(self.proposal, self.facts, self.primary, self.arithmetic)

    def test_full_long_context_uses_full_reservation_without_new_count_calls(self):
        result = provider_plan.load_full_context(HERE / "full-context-provider-alternatives.json")
        self.assertEqual(result["total_dispatches"], 432)
        self.assertEqual(result["count_endpoint_dispatches"], 0)
        self.assertEqual([row["maximum_reserved_usd"] for row in result["options"]],
                         ["1538.97467392", "1738.96187392", "2007.76187392"])
        self.assertFalse(result["execution_authorized"])
        self.assertEqual(self.arithmetic["maximum_reserved_usd"], "68.97467392")

    def test_old_seventy_five_dollar_cap_is_insufficient(self):
        self.proposal["options"][0]["proposed_usd_stop_cap"] = "75.00"
        with self.assertRaisesRegex(ValueError, "cannot cover"):
            self.review()

    def test_short_context_discount_cannot_price_the_model_maximum(self):
        self.facts["long_context_usd_per_million"] = {"input": "2.00", "cache_write": "2.50", "output": "10.00"}
        with self.assertRaisesRegex(ValueError, "long-context rates"):
            self.review()

    def test_ciphertext_size_or_output_subtraction_cannot_reduce_input_reservation(self):
        for tokens in [32768, 1050000 - 16384, True]:
            self.proposal["input_reservation_tokens_per_generation"] = tokens
            with self.subTest(tokens=tokens), self.assertRaises(ValueError):
                self.review()

    def test_call_corpus_retry_or_time_allowances_cannot_drift(self):
        original = copy.deepcopy(self.proposal)
        for field, value in [("generation_dispatches", 279), ("embedding_dispatches", 151),
                             ("count_endpoint_dispatches", 280), ("corpus_canonical_sha256", "0" * 64),
                             ("retries_consume_original_role_and_global_quotas", False)]:
            self.proposal = copy.deepcopy(original)
            self.proposal[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.review()
        self.proposal = original
        self.proposal["unchanged_limits"]["model_deadline_seconds"] += 1
        with self.assertRaises(ValueError):
            self.review()

    def test_output_transform_cannot_raise_caps_or_become_implicit(self):
        original = copy.deepcopy(self.proposal)
        for field, value in [("output_mode", "silent-truncation"), ("output_reservation_tokens", 128001)]:
            self.proposal = copy.deepcopy(original)
            self.proposal["options"][0][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.review()

    def test_alternative_cannot_select_itself_or_price_unknown_count_as_free(self):
        self.proposal["selected_option"] = self.proposal["options"][0]["id"]
        with self.assertRaisesRegex(ValueError, "cannot authorize"):
            self.review()
        self.proposal["selected_option"] = None
        self.proposal["count_option"]["count_billing_bound_usd"] = "0"
        with self.assertRaisesRegex(ValueError, "count economics"):
            self.review()


if __name__ == "__main__":
    unittest.main()
