"""Evaluator mutation tests only; synthetic captures are never product evidence."""
import copy
import unittest

from controls import DIMENSIONS, SEEDS, evidence, negative_control
from oracle import APPROVED_REVIEWER, digest, evaluate, evidence_id, load_corpus, score_case, text_digest, validate_corpus


def fixture_capture(corpus):
    """Construct a clearly labelled mathematical positive control for the oracle."""
    cases, reviews = [], []
    for query in corpus["queries"]:
        pairs = {(e["source_id"], e["revision"]) for e in corpus["evidence"]
                 if e["space"] == query["space"] and e["id"] in query["required"]}
        sources = [s for s in corpus["sources"] if s["space"] == query["space"]
                   and (s["id"], s["revision"]) in pairs]
        results = [{"kind": "claim", "text": "TEST FIXTURE: " + ", ".join(query["facts"]),
                    "evidence": [evidence(s) for s in sources]}] if sources else []
        response = {"results": results, "coverage": {s["id"]: {
            "requested_revision": s["revision"], "desired_revision": s["revision"],
            "published_revision": s["revision"], "state": "complete"} for s in sources}, "partial": False}
        cases.append({"id": query["id"], "query": query["query"], "space": query["space"],
                      "response": response, "fallback_fetches": 0})
        reviews.append({"id": query["id"], "response_sha256": digest(response), "claims": [{
            "result_index": 0, "text_sha256": text_digest(results[0]["text"]),
            "verdict": "supported", "facts": query["facts"], "evidence_ids": query["required"]}]
                       if results else []})
    capture = {"manifest": {"evidence_class": "fixture-only", "source_sha": "a" * 40,
                           "artifact_sha256": "b" * 64, "configuration_sha256": "c" * 64,
                           "provider_receipts_sha256": "d" * 64, "corpus_sha256": digest(corpus),
                           "backend": "surreal-db", "authentication_enabled": True,
                           "expectations_review_sha256": "e" * 64, "query_mode": "semantic-only",
                           "fallback_fetches": 0, "query_rewrites": 0}, "cases": cases}
    # A mathematical test of the accepted shape, not an actual D attestation.
    # fixture-only capture evidence remains ineligible regardless of these fields.
    review = {"status": "approved", "reviewer": APPROVED_REVIEWER, "fixture_only": True, "corpus_sha256": digest(corpus),
              "capture_sha256": digest(capture), "expectations_review_sha256": "e" * 64, "cases": reviews}
    return capture, review


class IndependentOracleTests(unittest.TestCase):
    def setUp(self):
        self.corpus = load_corpus()
        self.capture, self.review = fixture_capture(self.corpus)
        self.query = self.corpus["queries"][0]
        self.case = self.capture["cases"][0]
        self.judgement = self.review["cases"][0]

    def score(self):
        return score_case(self.corpus, self.query, self.case, self.judgement)

    def rebind_review(self):
        self.judgement["response_sha256"] = digest(self.case["response"])

    def test_positive_fixture_has_three_independent_axes_but_no_acceptance(self):
        report = evaluate(self.corpus, self.capture, self.review)
        self.assertTrue(report["retrieval_pass"])
        self.assertTrue(report["groundedness_pass"])
        self.assertTrue(report["completion_pass"])
        self.assertEqual(report["verdict"], "not-qualified")

    def test_invalid_corpus_raises_explicit_validation_error(self):
        self.corpus["metric"]["cutoff"] = 4
        with self.assertRaisesRegex(ValueError, "cutoff"):
            validate_corpus(self.corpus)

    def test_missing_coverage_for_returned_distractor_fails_completion(self):
        source = next(s for s in self.corpus["sources"]
                      if s["space"] == self.query["space"] and s["id"] != "home/water")
        self.case["response"]["results"].append({"kind": "chunk", "text": source["text"],
                                                   "evidence": [evidence(source)]})
        self.assertFalse(self.score()["completion_pass"])

    def test_invented_chunk_cannot_borrow_an_unrelated_valid_quote(self):
        self.query = next(q for q in self.corpus["queries"] if q.get("unsupported_question"))
        self.case["query"], self.case["space"] = self.query["query"], self.query["space"]
        result = self.case["response"]["results"][0]
        result["kind"] = "chunk"
        result["text"] = "An attic turbine was installed by an invented contractor."
        self.judgement["claims"] = []
        self.rebind_review()
        self.assertFalse(self.score()["groundedness_pass"])
        self.assertTrue(any("chunk text" in e for e in self.score()["grounding_errors"]))

    def test_exact_unrelated_chunk_is_valid_without_claim_review(self):
        self.query = next(q for q in self.corpus["queries"] if q.get("unsupported_question"))
        self.case["query"], self.case["space"] = self.query["query"], self.query["space"]
        result = self.case["response"]["results"][0]
        result["kind"] = "chunk"
        result["text"] = result["evidence"][0]["quote"]
        self.judgement["claims"] = []
        self.rebind_review()
        self.assertTrue(self.score()["groundedness_pass"])

    def test_malformed_source_identity_is_reported(self):
        self.case["response"]["results"][0]["evidence"][0]["source_id"] = []
        self.assertFalse(self.score()["groundedness_pass"])

    def test_malformed_review_claim_is_reported(self):
        self.judgement["claims"] = [None]
        self.assertFalse(self.score()["groundedness_pass"])

    def test_malformed_judgment_string_arrays_are_reported(self):
        for field in ("evidence_ids", "facts"):
            for value in (None, "string-instead-of-array", [[]], [{}], [False], [""]):
                with self.subTest(field=field, value=value):
                    review = copy.deepcopy(self.judgement)
                    review["claims"][0][field] = value
                    report = score_case(self.corpus, self.query, self.case, review)
                    self.assertFalse(report["groundedness_pass"])
                    self.assertTrue(any(field in error for error in report["grounding_errors"]))

    def test_empty_or_unknown_reviewer_is_not_approved(self):
        for identity in (None, "", "unappointed-reviewer", {}, []):
            with self.subTest(identity=identity):
                review = copy.deepcopy(self.review)
                review["reviewer"] = identity
                report = evaluate(self.corpus, self.capture, review)
                self.assertEqual(report["verdict"], "not-qualified")
                self.assertTrue(any("independent D review" in e for e in report["eligibility_errors"]))

    def test_null_coverage_item_is_reported(self):
        self.case["response"]["coverage"]["home/water"] = None
        self.assertFalse(self.score()["completion_pass"])

    def test_null_manifest_is_ineligible(self):
        self.capture["manifest"] = None
        self.assertEqual(evaluate(self.corpus, self.capture, self.review)["verdict"], "not-qualified")

    def test_seeded_fake_vectors_fail_numerical_semantic_expectations(self):
        for seed in SEEDS:
            for dims in DIMENSIONS:
                with self.subTest(seed=seed, dimensions=dims):
                    result = negative_control(self.corpus, seed, dims)
                    self.assertTrue(result["valid_finite_nonzero_unit_vectors"])
                    self.assertFalse(result["numerical_retrieval_pass"])
                    self.assertTrue(result["negative_control_pass"])
                    self.assertFalse(result["product_acceptance"])

    def test_missing_query_cannot_disappear_from_denominator(self):
        self.capture["cases"].pop()
        self.assertEqual(evaluate(self.corpus, self.capture, self.review)["verdict"], "not-qualified")

    def test_duplicate_query_invalidates_evidence(self):
        self.capture["cases"].append(copy.deepcopy(self.case))
        self.assertTrue(any("duplicate" in error for error in evaluate(self.corpus, self.capture)["eligibility_errors"]))

    def test_rewritten_query_fails(self):
        self.case["query"] += " home/water"
        self.assertFalse(self.score()["retrieval_pass"])

    def test_fallback_context_is_rejected(self):
        self.case["fallback_fetches"] = 1
        self.assertFalse(self.score()["retrieval_pass"])

    def test_wrong_space_is_rejected(self):
        self.case["space"] = "semantics-business"
        self.assertFalse(self.score()["retrieval_pass"])

    def test_empty_results_never_get_advisory_threshold(self):
        self.case["response"]["results"] = []
        self.assertFalse(self.score()["retrieval_pass"])
        self.assertFalse(self.score()["groundedness_pass"])

    def test_precision_keeps_fixed_denominator(self):
        self.assertEqual(self.score()["precision_at_3"], 1 / 3)

    def test_distractor_ranks_are_not_removed(self):
        distractors = [s for s in self.corpus["sources"] if s["space"] == self.query["space"] and s["id"] != "home/water"][:3]
        self.case["response"]["results"][:0] = [{"kind": "chunk", "text": s["text"], "evidence": [evidence(s)]} for s in distractors]
        self.assertFalse(self.score()["retrieval_pass"])

    def test_duplicate_source_does_not_inflate_precision(self):
        self.case["response"]["results"] *= 3
        self.assertEqual(self.score()["precision_at_3"], 1 / 3)

    def test_foreign_known_id_is_not_evidence(self):
        source = next(s for s in self.corpus["sources"] if s["space"] == "semantics-business")
        self.case["response"]["results"][0]["evidence"] = [evidence(source)]
        self.assertFalse(self.score()["groundedness_pass"])
        self.assertFalse(self.score()["retrieval_pass"])

    def test_tampered_quote_fails(self):
        self.case["response"]["results"][0]["evidence"][0]["quote"] = "fabricated quotation"
        self.assertFalse(self.score()["groundedness_pass"])

    def test_empty_span_fails(self):
        self.case["response"]["results"][0]["evidence"][0]["span"]["end"] = 0
        self.assertFalse(self.score()["groundedness_pass"])

    def test_bad_hash_fails_even_if_quote_matches(self):
        self.case["response"]["results"][0]["evidence"][0]["quote_hash"] = "derived:fake"
        self.assertFalse(self.score()["groundedness_pass"])

    def test_exact_quote_does_not_certify_entailment(self):
        self.judgement["claims"][0]["verdict"] = "unsupported"
        self.assertTrue(self.score()["retrieval_pass"])
        self.assertFalse(self.score()["groundedness_pass"])

    def test_forbidden_fact_fails_despite_supported_label(self):
        self.judgement["claims"][0]["facts"].append(self.query["forbidden_facts"][0])
        self.assertFalse(self.score()["groundedness_pass"])

    def test_unreviewed_additional_claim_fails(self):
        self.case["response"]["results"].append(copy.deepcopy(self.case["response"]["results"][0]))
        self.rebind_review()
        self.assertFalse(self.score()["groundedness_pass"])

    def test_claim_cannot_borrow_evidence_from_other_result(self):
        self.case["response"]["results"][0]["evidence"] = []
        self.rebind_review()
        self.assertFalse(self.score()["groundedness_pass"])

    def test_review_bound_to_actual_response(self):
        self.case["response"]["results"][0]["text"] = "changed claim"
        self.assertFalse(self.score()["groundedness_pass"])

    def test_pending_coverage_is_not_complete(self):
        self.case["response"]["coverage"]["home/water"]["state"] = "pending"
        self.assertFalse(self.score()["completion_pass"])

    def test_future_desired_revision_invalidates_current_completion(self):
        self.case["response"]["coverage"]["home/water"]["desired_revision"] = "water-r2"
        self.assertFalse(self.score()["completion_pass"])

    def test_coverage_array_is_rejected(self):
        self.case["response"]["coverage"] = list(self.case["response"]["coverage"].values())
        self.assertFalse(self.score()["completion_pass"])

    def test_partial_cannot_pass_completion(self):
        self.case["response"]["partial"] = True
        self.assertFalse(self.score()["completion_pass"])

    def test_no_support_query_cannot_assert_unsupported_answer(self):
        self.query = next(q for q in self.corpus["queries"] if q.get("unsupported_question"))
        self.case["query"], self.case["space"] = self.query["query"], self.query["space"]
        self.judgement["claims"][0]["facts"] = self.query["forbidden_facts"]
        self.assertFalse(self.score()["groundedness_pass"])

    def test_unsupported_question_can_return_unrelated_supported_search_hit(self):
        self.query = next(q for q in self.corpus["queries"] if q.get("unsupported_question"))
        self.case["query"], self.case["space"] = self.query["query"], self.query["space"]
        self.assertTrue(self.score()["groundedness_pass"])

    def test_exact_supporting_subspan_is_sufficient(self):
        passage = next(e for e in self.corpus["evidence"] if e["id"] == self.query["required"][0])
        ref = self.case["response"]["results"][0]["evidence"][0]
        ref["span"] = {"start": passage["start"], "end": passage["end"]}
        ref["quote"] = passage["quote"]
        self.rebind_review()
        self.assertTrue(self.score()["retrieval_pass"])
        self.assertTrue(self.score()["groundedness_pass"])

    def test_late_support_cannot_credit_earlier_irrelevant_same_source_span(self):
        result = self.case["response"]["results"][0]
        supporting = copy.deepcopy(result["evidence"][0])
        raw = supporting["quote"].encode()
        unrelated = b"The blue wheel isolates only the upstairs washbasin."
        start = raw.index(unrelated)
        result["evidence"][0]["span"] = {"start": start, "end": len(raw)}
        result["evidence"][0]["quote"] = unrelated.decode()
        distractors = [s for s in self.corpus["sources"] if s["space"] == self.query["space"]
                       and s["id"] != "home/water"][:2]
        result["evidence"].extend(evidence(s) for s in distractors)
        result["evidence"].append(supporting)
        # Expected support is actually the fourth wire citation; the old
        # global-set implementation incorrectly rescued source rank one.
        self.assertFalse(self.score()["retrieval_pass"])
        self.assertEqual(self.score()["precision_at_3"], 0)

    def test_stale_revision_cannot_masquerade_as_current(self):
        self.query = next(q for q in self.corpus["queries"] if q["id"] == "h-visit")
        old = next(s for s in self.corpus["sources"] if s.get("state") == "superseded")
        self.case["query"], self.case["space"] = self.query["query"], self.query["space"]
        self.case["response"]["results"][0]["evidence"] = [evidence(old)]
        self.assertFalse(self.score()["groundedness_pass"])

    def test_hybrid_retrieval_cannot_establish_vector_semantics(self):
        self.capture["manifest"]["query_mode"] = "hybrid"
        self.review["capture_sha256"] = digest(self.capture)
        self.assertTrue(any("semantic-only" in e for e in evaluate(self.corpus, self.capture, self.review)["eligibility_errors"]))

    def test_changed_corpus_requires_new_review(self):
        self.corpus["queries"][0]["query"] += " changed"
        self.assertEqual(evaluate(self.corpus, self.capture, self.review)["verdict"], "not-qualified")


if __name__ == "__main__":
    unittest.main()
