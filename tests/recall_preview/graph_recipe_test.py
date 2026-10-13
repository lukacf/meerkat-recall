"""Independent literal recipe controls; no service, database, or model execution."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
DIRECTORY = Path(__file__).with_name("graph-recipes")
sys.path.insert(0, str(ROOT / "tests/recall_preview_semantics"))
from full_workload_graph import validate_recipe as validate_graph
from full_workload_spy import support_response, validate_recipe as validate_spy
from full_workload_transport import Unmet, encode, sha


def read(name):
    return json.loads((DIRECTORY / name).read_bytes())


def complete_semantic(row):
    return {
        "subject": row["subject_text"], "predicate": row["predicate_raw"],
        "object": row["object_text"], "literal": row["object_literal"],
        "subject_type": row["subject_entity_type"], "object_type": row["object_entity_type"],
        "subject_entity": None, "object_entity": None, "event_time": None,
        "event_precision": None, "event_title": None, "event_lineage": None,
        "confidence": 1.0, "significance": "medium",
    }


class LiteralRecipeTests(unittest.TestCase):
    def setUp(self):
        for name in ("asyncio.start_server", "asyncio.open_connection", "socket.socket", "subprocess.Popen"):
            guard = patch(name, side_effect=AssertionError("literal recipe controls cannot execute runtime"))
            guard.start()
            self.addCleanup(guard.stop)
        self.manifest = read("manifest.json")

    def test_complete_file_inventory_and_independent_original_contract_hashes(self):
        self.assertFalse(self.manifest["execution_authorized"])
        self.assertFalse(self.manifest["provider_dispatch_authorized"])
        self.assertFalse(self.manifest["product_acceptance"])
        self.assertEqual(self.manifest["review_status"], "pending_qualification_owner")
        self.assertEqual(len(self.manifest["recipes"]), 28)
        self.assertEqual(self.manifest["frozen_contracts"]["workload-corpus.json"],
                         "2192437954109fc17ebbea5b3960eb6b398a2ee91a725ce70666b119e86179dc")
        self.assertEqual(self.manifest["frozen_contracts"]["full-workload-contract.json"],
                         "44a91b9f0ae4c2ae32d2f8aec9eb783a48ccca63eeee010ff2427ef1ec4a0c0b")
        for name, digest in self.manifest["frozen_contracts"].items():
            self.assertEqual(sha((ROOT / "tests/recall_preview" / name).read_bytes()), digest)
        paths = set()
        entries = self.manifest["recipes"] + self.manifest["spy_recipes"] + [self.manifest["response_budget_arithmetic"]]
        for entry in entries:
            self.assertNotIn(entry["path"], paths)
            paths.add(entry["path"])
            raw = (DIRECTORY / entry["path"]).read_bytes()
            self.assertEqual(len(raw), entry["bytes"])
            self.assertLessEqual(len(raw), 8 * 1024**2)
            self.assertEqual(sha(raw), entry["file_sha256"])
            self.assertEqual(sha(encode(json.loads(raw))), entry["canonical_sha256"])
        self.assertEqual(paths | {"manifest.json"}, {p.name for p in DIRECTORY.glob("*.json")})

    def test_every_literal_source_span_hash_and_typed_policy(self):
        for entry in self.manifest["recipes"]:
            recipe = read(entry["path"])
            with self.subTest(entry=entry["id"]):
                validate_graph(recipe, entry["canonical_sha256"])
                self.assertNotIn("expected_generation", recipe["source_request"]["intake"])
                source = recipe["source_request"]["source"]
                self.assertEqual(source["space_id"], entry["space"])
                self.assertEqual(source["external_id"], entry["external_key"])
                self.assertEqual(recipe["source_request"]["intake"]["revision_id"], entry["revision"])
                level = 3 if entry["space"] == "qualification-household" else 7 if entry["space"] == "qualification-business" else 0
                self.assertEqual(source["security"]["level"], level)
                self.assertEqual(source["subjects"], [])
                self.assertEqual(source["subjects_state"], "unknown")
                raw = recipe["source_request"]["doc"]["text"]
                self.assertEqual(recipe["rendered_text"], raw if raw.endswith("\n") else raw + "\n")

    def test_original_fifty_sources_and_exact_cas_identity_are_retained(self):
        corpus = json.loads((ROOT / "tests/recall_preview/workload-corpus.json").read_bytes())
        originals = {(r["space"], r["key"]): r["text"] for r in corpus["sources"]}
        self.assertEqual(len(originals), 50)
        identities = set(originals)
        entries = [e for e in self.manifest["recipes"] if e["space"].startswith("qualification-")]
        self.assertEqual(len(entries), 24)
        for entry in entries:
            recipe = read(entry["path"])
            identity = (entry["space"], entry["external_key"])
            identities.add(identity)
            if identity in originals:
                self.assertTrue(recipe["source_request"]["doc"]["text"].startswith(originals[identity] + "\n"))
                self.assertEqual(entry["original_generation"], "captured_original_head_generation")
                self.assertEqual(len(recipe["expected_assertions"]), 1)
                self.assertEqual(recipe["expected_assertions"][0]["start"], len(originals[identity].encode()) + 1)
        self.assertEqual(len(identities), 56)
        for short in ("household", "business"):
            a, b = (read(f"{short}-fw08-{suffix}.json")["source_request"] for suffix in ("a", "b"))
            self.assertEqual(a["source"]["external_id"], "workload/atlas-03")
            self.assertEqual(a["source"]["external_id"], b["source"]["external_id"])
            self.assertNotEqual(a["intake"], b["intake"])
            self.assertNotEqual(a["doc"]["text"], b["doc"]["text"])

    def test_both_shared_facts_remain_literal_and_have_independent_support(self):
        text = "Mira maintains the Helios shared notebook. The notebook access code is violet-47."
        for short in ("household", "business"):
            pair = [read(f"{short}-fw04-{suffix}.json") for suffix in ("a", "b")]
            self.assertNotEqual(pair[0]["source_request"]["source"]["external_id"], pair[1]["source_request"]["source"]["external_id"])
            for recipe in pair:
                self.assertEqual(recipe["source_request"]["doc"]["text"], text)
                rows = recipe["expected_assertions"]
                self.assertEqual(len(rows), 2)
                self.assertEqual((rows[0]["subject_text"], rows[0]["predicate_raw"], rows[0]["object_text"]),
                                 ("Mira", "maintains", "Helios shared notebook"))
                self.assertEqual((rows[1]["subject_text"], rows[1]["predicate_raw"], rows[1]["object_literal"]),
                                 ("Helios shared notebook", "access_code", {"type": "string", "value": "violet-47"}))

    def test_actual_maximum_original_bytes_and_fixed_terminal_evidence(self):
        expected_hash = "fdae2b405d6f3123836e3dc6b6aef46de8cf41342349265ff06a403d1f1af613"
        for short in ("household", "business"):
            recipe = read(f"{short}-large-graph.json")
            raw = recipe["source_request"]["doc"]["text"].encode()
            self.assertEqual(len(raw), 2_000_000)
            self.assertEqual(sha(raw), expected_hash)
            self.assertEqual(recipe["rendered_sha256"], expected_hash)
            self.assertEqual(recipe["expected_assertions"][0]["start"], 0)
            final = recipe["expected_assertions"][1]
            self.assertEqual(final["end"], 1_999_999)
            self.assertEqual(final["quote"], "Helios final original evidence: Mira keeps the violet key; capacity is 47.")
            self.assertEqual(raw[final["start"]:final["end"]].decode(), final["quote"])

    def test_spy_schedules_and_support_never_adopt_unknown_semantics(self):
        for entry in self.manifest["spy_recipes"]:
            spy = validate_spy(read(entry["path"]), entry["canonical_sha256"])
            self.assertLessEqual(sum(r["maximum_calls"] for r in spy["rules"]), 4096)
            for rule in spy["rules"]:
                if rule["mode"] != "support":
                    continue
                for expected in rule["output"]:
                    claim = {**expected["semantic"], "revision": "doc_revision:11111111-1111-4111-8111-111111111111",
                             "node": None, "start": 0, "end": len(expected["quote"].encode()), "quote_hash": sha(expected["quote"].encode())}
                    candidate = [{"index": 7, "claim": claim, "quote": expected["quote"]}]
                    self.assertEqual(support_response([encode(candidate).decode()], rule["output"]),
                                     {"decisions": [{"index": 7, "supported": True}]})
                    candidate[0]["claim"]["predicate"] = "unsupported alternative interpretation"
                    self.assertEqual(support_response([encode(candidate).decode()], rule["output"]),
                                     {"decisions": [{"index": 7, "supported": False}]})
        workload = read("workload-spy.json")
        for short in ("household", "business"):
            for suffix in ("fw06", "fw07", "fw09-running", "fw11"):
                rule = next(r for r in workload["rules"] if r["id"] == short + "-" + suffix + "-extract")
                self.assertEqual(rule["hold_calls"], [1])
            retry = next(r for r in workload["rules"] if r["id"] == short + "-fw11-extract")
            self.assertEqual(retry["statuses"], [503, 503, 503])

    def test_supported_response_budget_arithmetic_and_old_case_stays_unmet(self):
        recipes = [read(f"container-response-budget-{group}.json") for group in range(2)]
        arithmetic = read("container-response-budget-arithmetic.json")
        rows = [row for recipe in recipes for row in recipe["expected_assertions"]]
        self.assertEqual(len(rows), 20)
        for recipe in recipes:
            self.assertLess(len(encode(recipe["source_request"]["source"])) + len(encode(recipe["source_request"]["doc"]["text"])), 270336)
            self.assertEqual(len(recipe["source_request"]["doc"]["text"].encode()), 65018)
        for index, row in enumerate(rows):
            self.assertEqual(row["start"], (index % 10) * 6502)
            self.assertEqual(row["end"] - row["start"], 6500)
            self.assertEqual(row["quote"].count("\u0001"), 3000)
            self.assertEqual(len(encode(row["quote"])), 21502)
            self.assertEqual(row["predicate_raw"], "oversees")
            self.assertNotIn("oversees", row["subject_text"])
            self.assertNotIn("oversees", row["object_text"])
        self.assertEqual(arithmetic["limit20"]["quote_only_response_bytes_lower_bound"], 387036)
        self.assertGreater(387036, 262144)
        self.assertEqual(arithmetic["limit10"]["full_response_bytes_upper_bound"], 221584)
        self.assertLess(221584, 262144)
        old = next(e for e in self.manifest["recipes"] if e["id"] == "container-oversized-unmet")
        self.assertEqual(old["eligibility"], "unmet_default_extraction_window_12000_bytes")
        self.assertGreater(read(old["path"])["expected_assertions"][0]["end"], 12000)
        self.assertNotIn(read(old["path"])["extract_rule_id"], {r["id"] for r in read("container-spy.json")["rules"]})

    def test_dual_coordinate_budget_counts_complete_rest_and_mcp_encoding(self):
        arithmetic = read("container-response-budget-arithmetic.json")
        recipe = read("container-response-budget-1.json")
        fact = recipe["expected_assertions"][-1]
        # A fixed conservative size witness, not a proposed physical identity or
        # semantic expectation. Maximum coordinate widths cover every real span.
        score = 1.7976931348623157e308
        score_growth = 32 - len(encode(score))
        evidence = {
            "source_id": "boundary/graph-response-budget-1", "revision": "boundary-response-r1",
            "span": {"start": 65018, "end": 65019}, "quote": fact["quote"],
            "doc_id": "doc:ffffffff-ffff-ffff-ffff-ffffffffffff",
            "internal_doc_revision_id": "doc_revision:ffffffff-ffff-ffff-ffff-ffffffffffff",
            "rendered_span": {"start": 65018, "end": 65019},
            "rendered_quote_sha256": "f" * 64, "rendered_text_sha256": "f" * 64,
        }
        claim = {"kind": "claim", "text": "Boundary custodian 19 oversees Boundary notebook 19",
                 "evidence": [evidence], "score": score}
        chunk = {"kind": "chunk", "text": "\u0001" * 512,
                 "evidence": [{**evidence, "quote": "\u0001" * 512}], "score": score}
        coverage = {
            key: {"requested_revision": revision, "published_revision": revision,
                  "desired_revision": revision, "state": "complete"}
            for key, revision in arithmetic["source_revision_constraints"].items()
        }
        body = {"results": [claim] * 10, "coverage": coverage, "partial": False}
        tool_response = {"ok": True, "data": body}
        mcp = {"jsonrpc": "2.0", "id": 18446744073709551615,
               "result": {"content": [{"type": "text", "text": encode(tool_response).decode()}], "isError": False}}
        limits = arithmetic["limit10"]
        self.assertEqual(limits["maximum_claim_result_encoded_bytes"], len(encode(claim)) + score_growth)
        self.assertEqual(limits["maximum_chunk_result_encoded_bytes"], len(encode(chunk)) + score_growth)
        self.assertLess(limits["maximum_chunk_result_encoded_bytes"], limits["maximum_claim_result_encoded_bytes"])
        self.assertEqual(limits["full_response_bytes_upper_bound"], len(encode(body)) + 10 * score_growth)
        self.assertEqual(limits["mcp_tool_response_bytes_upper_bound"],
                         len(encode(tool_response)) + 10 * score_growth)
        self.assertLess(limits["mcp_tool_response_bytes_upper_bound"], limits["response_budget_bytes"])
        self.assertEqual(limits["mcp_jsonrpc_bytes_upper_bound"], len(encode(mcp)) + 10 * score_growth)
        self.assertEqual(limits["mcp_streamed_capture_bytes_upper_bound"],
                         limits["mcp_jsonrpc_bytes_upper_bound"] + 4096)
        self.assertLess(limits["mcp_streamed_capture_bytes_upper_bound"], limits["original_collector_response_bytes"])
        self.assertLess(limits["full_response_bytes_upper_bound"], limits["response_budget_bytes"])
        contract = arithmetic["wire_contract"]
        self.assertEqual(contract["required_additive_fields"],
                         ["rendered_span", "rendered_quote_sha256", "rendered_text_sha256", "doc_id"])
        self.assertEqual(contract["internal_revision_id_encoded_string_bytes"], len(encode(evidence["internal_doc_revision_id"])))
        self.assertEqual(contract["doc_id_encoded_string_bytes"], len(encode(evidence["doc_id"])))
        self.assertEqual(len(evidence), 9)
        without_parent = {key: value for key, value in evidence.items() if key != "doc_id"}
        self.assertEqual(len(encode(evidence)) - len(encode(without_parent)), 52)
        self.assertEqual(len(encode(encode(evidence).decode())) - len(encode(encode(without_parent).decode())), 56)
        self.assertNotIn("rendered_quote", evidence)

    def test_changed_original_source_span_or_literal_cannot_keep_frozen_hash(self):
        entry = next(e for e in self.manifest["recipes"] if e["id"] == "household-fw04-a")
        for mutation in ("source", "span", "literal", "generation"):
            recipe = copy.deepcopy(read(entry["path"]))
            if mutation == "source":
                recipe["source_request"]["doc"]["text"] += " changed"
            elif mutation == "span":
                recipe["expected_assertions"][0]["end"] -= 1
            elif mutation == "literal":
                recipe["expected_assertions"][1]["object_literal"]["value"] = "amber-49"
            else:
                recipe["source_request"]["intake"]["expected_generation"] = "invented-current-generation"
            with self.subTest(mutation=mutation), self.assertRaises(Unmet):
                validate_graph(recipe, entry["canonical_sha256"])


if __name__ == "__main__":
    unittest.main()
