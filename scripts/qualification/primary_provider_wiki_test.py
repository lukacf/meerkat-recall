"""Synthetic public-wire controls only; no service, provider or process launch."""
from __future__ import annotations

import copy
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import primary_provider_driver as driver
import primary_provider_driver_contract as contract


def record(table, number=1):
    return table + ":00000000-0000-0000-0000-" + str(number).zfill(12)


def fixture():
    space = "semantics-household"
    security = {"policy_id": "synthetic-household", "level": 3, "labels": [], "handling": []}
    definition_request = {"title": "Dishwasher repair", "publication_status": "published", "related_articles": [],
                          "selector": {"type": "query", "entity_types": [], "name_contains": "dishwasher",
                                       "attributes": [], "scan_limit": 256, "max_members": 64}, "security": security}
    case = {"id": "primary-household-wiki", "category": "wiki", "space": space,
            "tool": "compile_wiki_article", "arguments": {"publish": True}, "definition": definition_request}
    cases = {"format": "recall-primary-conditional-cases/v1", "reviewer": "qualification (independent D lane)",
             "status": "approved-before-execution", "cases": [case], "disposition": {
                 "wiki": {"status": "required", "reason": "synthetic scoped article control"},
                 "truth_maintenance": {"status": "not-applicable", "reason": "synthetic control only"}}}
    base = {"space_id": space, "security": security, "deleted_at": None,
            "subjects": [record("entity")], "subjects_state": "complete", "updated_at": "2026-10-07T00:00:00Z"}
    definition = {**copy.deepcopy(base), **copy.deepcopy(definition_request),
                  "id": record("knowledge_view_definition"), "subjects": [], "navigation": None}
    dependencies = [{"record_id": record("knowledge_view_definition"), "record_type": "knowledge_view_definition",
                     "role": "root", "observed_updated_at": base["updated_at"]},
                    {"record_id": record("entity"), "record_type": "entity", "role": "selected_member",
                     "observed_updated_at": base["updated_at"]},
                    {"record_id": record("source"), "record_type": "source", "role": "original_source"},
                    {"record_id": record("doc_revision"), "record_type": "doc_revision", "role": "evidence_span"}]
    statement = {"id": "member", "text": "Dishwasher matches this context.", "derivation": "direct",
                 "verification": "deterministic_record_rendering", "citations": ["member"]}
    context = {"changed": True, "stale_fallback": False,
               "view": {**copy.deepcopy(base), "id": record("knowledge_view"), "kind": "query",
                        "surface": "inspector", "publication_status": "draft", "state": "current",
                        "root_record": definition["id"], "current_revision": record("knowledge_view_revision")},
               "revision": {**copy.deepcopy(base), "id": record("knowledge_view_revision"),
                            "view_id": record("knowledge_view"), "kind": "query", "root_record": definition["id"],
                            "dependencies": dependencies,
                            "citations": [{"id": "member", "kind": "graph_record", "record_id": record("entity")}],
                            "blocks": [{"id": "members", "kind": "members", "statements": [statement],
                                        "citations": ["member"], "markdown": "Dishwasher matches this context. [^member]"}],
                            "freshness": {"state": "current", "selection_truncated": False, "stale_truth_slots": 0,
                                          "unresolved_conflicts": 0, "unsupported_claims": 0, "expired_claims": 0},
                            "validation": {"citations_resolve": True, "factual_blocks_cited": True,
                                           "statements_supported": True, "prose_is_structured": True,
                                           "warnings": [], "factual_block_count": 1, "citation_count": 1, "statement_count": 1}}}
    text = "The repair quote is 420 EUR.\n"
    original = {"internal_source_id": record("source"), "doc_id": record("doc"), "operation_id": record("source_operation")}
    publication = {"case": [space, "home/repair", "repair-r1"], "original": original,
                   "pipeline": {"revision": record("doc_revision")},
                   "nodes": [{"id": record("doc_node"), "span": {"start": 0, "end": len(text.encode())}}]}
    rendered = {"sources": [{"space": space, "source_key": "home/repair", "revision": "repair-r1", "rendered_text": text}]}
    current = {(space, "home/repair"): original}
    article = copy.deepcopy(context)
    article.pop("stale_fallback")
    article["view"].update(id=record("knowledge_view", 2), current_revision=record("knowledge_view_revision", 2),
                           kind="article", surface="wiki", publication_status="published", title="Dishwasher repair")
    article["revision"].update(id=record("knowledge_view_revision", 2), view_id=record("knowledge_view", 2), kind="article")
    article["revision"]["citations"] = [{"id": "quote", "kind": "evidence_span", "record_id": record("doc_revision"),
                                          "source_id": record("source"), "evidence_span": {
                                              "revision": record("doc_revision"), "node": record("doc_node"),
                                              "start": 0, "end": len(text.encode()), "quote_hash": contract.sha(text.encode())}}]
    article["revision"]["blocks"] = [{"id": "lead", "kind": "overview", "heading": "Overview", "citations": ["quote"],
                                      "statements": [{"id": "lead", "text": text.strip(), "derivation": "synthesized",
                                                      "verification": "evidence_entailment", "citations": ["quote"]}],
                                      "markdown": text.strip() + " [^quote]"}]
    article["revision"]["rendered_markdown"] = "# Dishwasher repair\n\n## Overview\n\nThe repair quote is 420 EUR. [^quote]\n"
    return case, cases, definition, context, article, [publication], current, rendered


class NoExternalEffects(unittest.TestCase):
    def setUp(self):
        for target in ("socket.socket", "socket.create_connection", "subprocess.Popen"):
            self.enterContext(patch(target, side_effect=AssertionError("synthetic wiki control cannot launch external effects")))


class ScopedWikiContracts(NoExternalEffects):
    def test_frozen_case_and_optional_grant_marker_are_bound_both_directions(self):
        case, cases, *_ = fixture()
        self.assertEqual(contract.conditional_contract(cases, {case["space"]}), [case])
        contract.scoped_wiki_plan_binding([case], {"scoped_wiki": True})
        contract.scoped_wiki_plan_binding([], {})
        for selected, plan in (([case], {}), ([], {"scoped_wiki": True}),
                               ([case], {"scoped_wiki": 1}), ([], {"scoped_wiki": False})):
            with self.subTest(plan=plan), self.assertRaises(ValueError):
                contract.scoped_wiki_plan_binding(selected, plan)
        mutations = [lambda row: row["arguments"].update(definition_id=record("knowledge_view_definition")),
                     lambda row: row["arguments"].update(writer_model="different"),
                     lambda row: row["arguments"].update(publish=1),
                     lambda row: row["definition"]["selector"].update(name_contains="other"),
                     lambda row: row["definition"]["selector"].update(max_members=65),
                     lambda row: row["definition"]["selector"].update(scan_limit=256.0),
                     lambda row: row["definition"]["security"].update(level=2),
                     lambda row: row["definition"].update(definition_id=record("knowledge_view_definition")),
                     lambda row: row.update(extra="another command")]
        for mutate in mutations:
            wrong = copy.deepcopy(cases)
            mutate(wrong["cases"][0])
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                contract.conditional_contract(wrong, {case["space"]})

    def test_legacy_full_graph_case_remains_a_distinct_supported_path(self):
        case, cases, *_ = fixture()
        case.pop("definition")
        case.update(tool="create_wiki_from_graph", arguments={"manage": False, "publish": True,
                                                            "max_articles": 2, "article_max_hops": 2, "article_max_members": 64})
        self.assertEqual(contract.conditional_contract(cases, {case["space"]}), [case])
        contract.scoped_wiki_plan_binding([case], {})

    def test_actual_wire_context_draft_article_published_and_exact_span_evidence(self):
        case, _, definition, context, article, publications, current, rendered = fixture()
        originals = contract.wiki_publication_inputs(case["space"], publications, current, rendered)
        contract.wiki_projection_evidence(case, definition, context, originals)
        receipt = contract.wiki_projection_evidence(case, definition, article, originals, context=context)
        self.assertEqual(receipt["definition_id"], definition["id"])
        self.assertEqual(receipt["grounded_citation_ids"], ["quote"])
        self.assertEqual(receipt["full_graph_planner"], "unexercised")
        self.assertEqual(receipt["full_graph_organizer"], "unexercised")
        span = article["revision"]["citations"][0]["evidence_span"]
        span["quote_hash"] = "sha256:" + span["quote_hash"]
        contract.wiki_projection_evidence(case, definition, article, originals, context=context)

    def test_noncurrent_publication_cannot_supply_a_source_or_immutable_revision(self):
        case, _, _, _, _, publications, current, rendered = fixture()
        current[(case["space"], "home/repair")] = {**current[(case["space"], "home/repair")], "operation_id": "changed"}
        with self.assertRaisesRegex(ValueError, "no completed original"):
            contract.wiki_publication_inputs(case["space"], publications, current, rendered)
        with self.assertRaisesRegex(ValueError, "no completed original"):
            contract.wiki_publication_inputs("semantics-business", publications, current, rendered)

    def test_dirty_empty_truncated_unsupported_or_substituted_context_refuses(self):
        case, _, definition, context, _, publications, current, rendered = fixture()
        originals = contract.wiki_publication_inputs(case["space"], publications, current, rendered)
        mutations = [lambda row: row.update(stale_fallback=True),
                     lambda row: row["view"].update(root_record=record("knowledge_view_definition", 2)),
                     lambda row: row["view"].update(current_revision=record("knowledge_view_revision", 2)),
                     lambda row: row["view"].update(state="dirty"),
                     lambda row: row["revision"]["freshness"].update(selection_truncated=True),
                     lambda row: row["revision"]["freshness"].update(unsupported_claims=1),
                     lambda row: row["revision"]["validation"].update(warnings=["not clean"]),
                     lambda row: row["revision"].update(subjects_state="partial"),
                     lambda row: row["revision"].update(dependencies=[]),
                     lambda row: row["revision"]["dependencies"][1].update(role="unselected"),
                     lambda row: row["revision"]["dependencies"][2].update(record_id=record("source", 2))]
        for mutate in mutations:
            wrong = copy.deepcopy(context)
            mutate(wrong)
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                contract.wiki_projection_evidence(case, definition, wrong, originals)

    def test_article_receipt_cannot_hide_changed_versions_false_support_or_added_prose(self):
        case, _, definition, context, article, publications, current, rendered = fixture()
        originals = contract.wiki_publication_inputs(case["space"], publications, current, rendered)
        mutations = [lambda row: row.update(changed=False),
                     lambda row: row["view"].update(publication_status="draft"),
                     lambda row: row["revision"]["dependencies"][1].update(observed_updated_at="2026-10-08T00:00:00Z"),
                     lambda row: row["revision"]["citations"][0].update(source_id=record("source", 2)),
                     lambda row: row["revision"]["citations"][0]["evidence_span"].update(quote_hash="0" * 64),
                     lambda row: row["revision"]["citations"][0]["evidence_span"].update(end=100000),
                     lambda row: row["revision"]["citations"][0]["evidence_span"].update(node=record("doc_node", 2)),
                     lambda row: row["revision"]["blocks"][0]["statements"][0].update(verification="deterministic_record_rendering"),
                     lambda row: row["revision"]["blocks"][0].update(markdown="An appointment was booked."),
                     lambda row: row["revision"]["validation"].update(statement_count=2)]
        for mutate in mutations:
            wrong = copy.deepcopy(article)
            mutate(wrong)
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                contract.wiki_projection_evidence(case, definition, wrong, originals, context=context)

    def test_complete_rendered_article_cannot_add_or_replace_supported_prose(self):
        case, _, definition, context, article, publications, current, rendered = fixture()
        originals = contract.wiki_publication_inputs(case["space"], publications, current, rendered)
        article["view"]["title"] = "\u2003Dishwasher repair\u3000"
        contract.wiki_projection_evidence(case, definition, article, originals, context=context)
        # Each mutation leaves every original statement, citation and block
        # body intact. The complete public article still has to match them.
        mutations = [lambda row: row["revision"].update(rendered_markdown=
                         row["revision"]["rendered_markdown"] + "An appointment was booked.\n"),
                     lambda row: row["revision"].update(rendered_markdown="An appointment was booked.\n"),
                     lambda row: row["revision"].pop("rendered_markdown"),
                     lambda row: row["view"].update(title="Confirmed appointment"),
                     lambda row: row["revision"]["blocks"][0].update(heading="Confirmed appointment")]
        for mutate in mutations:
            wrong = copy.deepcopy(article)
            mutate(wrong)
            with self.subTest(mutation=mutate), self.assertRaisesRegex(ValueError, "wiki rendered article differs"):
                contract.wiki_projection_evidence(case, definition, wrong, originals, context=context)


class ScopedWikiCollector(NoExternalEffects):
    def run_synthetic(self, *, failure=None, context_change=None, costs=None, outer_deadline=1000.0, maintenance=False):
        case, cases, definition, context, article, publications, current, rendered = fixture()
        if maintenance:
            case.pop("definition")
            case.update(category="truth_maintenance", tool="create_reprocess_job",
                        arguments={"target_stages": ["re-maintain"], "budget_limit_euros": None, "max_sources_per_day": 28})
            cases["disposition"] = {"truth_maintenance": {"status": "required", "reason": "synthetic"},
                                     "wiki": {"status": "not-applicable", "reason": "synthetic"}}
        loaded = {"corpus": {"sources": []}, "conditional_cases": cases, "rendered_map": rendered,
                  "configurations": {case["space"]: {"source_security": definition["security"]}}}
        primary = driver.Primary({}, loaded, SimpleNamespace(deadline=outer_deadline))
        primary.publications, primary.current = publications, current
        now, calls = [100.0], []

        async def rpc(space, method, params, timeout=15):
            calls.append((method, copy.deepcopy(params), timeout))
            now[0] += (costs or {}).get(method, 0)
            return {}

        async def tool(space, name, args, timeout=15):
            calls.append((name, copy.deepcopy(args), timeout))
            now[0] += (costs or {}).get(name, 0)
            if failure == name:
                raise ValueError("synthetic refusal")
            if name == "upsert_knowledge_view_definition":
                return copy.deepcopy(definition)
            if name == "get_knowledge_view_definition":
                result = copy.deepcopy(definition)
                if failure == "readback-substitution":
                    result["id"] = record("knowledge_view_definition", 9)
                return result
            if name == "get_knowledge_view":
                result = copy.deepcopy(context)
                if context_change:
                    context_change(result)
                return result
            if name == "compile_wiki_article":
                return copy.deepcopy(article)
            if name == "create_reprocess_job":
                return {"id": record("reprocess_job")}
            if name == "get_reprocess_job_progress":
                return {"id": record("reprocess_job"), "status": "completed"}
            raise AssertionError("unexpected synthetic call")

        with patch.object(primary, "rpc", side_effect=rpc), patch.object(primary, "tool", side_effect=tool), \
                patch.object(driver.time, "monotonic", side_effect=lambda: now[0]):
            # These immediate synthetic responses never suspend. Driving the
            # coroutine directly also keeps asyncio's loop socketpair out of
            # the no-socket fixture; unexpected suspension fails this control.
            pending = primary.conditional()
            try:
                with self.assertRaises(StopIteration):
                    pending.send(None)
            finally:
                pending.close()
        return primary.conditionals[0], calls

    def test_four_public_calls_use_only_observed_definition_and_preserve_original_results(self):
        result, calls = self.run_synthetic()
        self.assertEqual(result["status"], "complete")
        self.assertEqual([row[0] for row in calls], ["initialize", "upsert_knowledge_view_definition",
                                                  "get_knowledge_view_definition", "get_knowledge_view", "compile_wiki_article"])
        self.assertNotIn("definition_id", calls[1][1])
        for _, arguments, _ in calls[2:]:
            self.assertEqual(arguments["definition_id"], result["original_definition_id"])
        self.assertEqual(calls[3][1], {"definition_id": record("knowledge_view_definition"), "freshness": "required", "limit": 64})
        self.assertEqual(calls[4][1], {"definition_id": record("knowledge_view_definition"), "publish": True})
        self.assertEqual(len(result["preparation"]), 3)
        self.assertEqual(result["coverage"]["full_graph_planner"], "unexercised")
        self.assertEqual(result["original_result"]["revision"]["kind"], "article")

    def test_each_refusal_stops_without_retry_replacement_or_compiler_fallback(self):
        for step in ("upsert_knowledge_view_definition", "get_knowledge_view_definition", "get_knowledge_view",
                     "compile_wiki_article", "readback-substitution"):
            result, calls = self.run_synthetic(failure=step)
            with self.subTest(step=step):
                self.assertEqual(result["status"], "incomplete")
                self.assertIn("error_type", result)
                self.assertEqual(len(calls), len({row[0] for row in calls}))
                if step != "compile_wiki_article":
                    self.assertNotIn("compile_wiki_article", [row[0] for row in calls])

    def test_bad_context_refuses_before_any_article_compiler_call(self):
        for change in (lambda row: row.update(stale_fallback=True),
                       lambda row: row["revision"]["freshness"].update(selection_truncated=True),
                       lambda row: row["revision"]["dependencies"][1].update(role="unselected")):
            result, calls = self.run_synthetic(context_change=change)
            self.assertEqual(result["status"], "incomplete")
            self.assertNotIn("compile_wiki_article", [row[0] for row in calls])

    def test_initialization_preparation_and_compilation_share_original_absolute_deadline(self):
        result, calls = self.run_synthetic(costs={"initialize": 5, "upsert_knowledge_view_definition": 10,
                                                "get_knowledge_view_definition": 10, "get_knowledge_view": 10})
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["original_deadline_monotonic"], 820.0)
        self.assertEqual(calls[-1][2], 685.0)
        result, calls = self.run_synthetic(costs={"initialize": 5}, outer_deadline=108.0)
        self.assertEqual(result["status"], "complete")
        self.assertTrue(all(row[2] <= 3 for row in calls[1:]))
        result, calls = self.run_synthetic(costs={"get_knowledge_view": 721})
        self.assertEqual(result["status"], "incomplete")
        self.assertNotIn("compile_wiki_article", [row[0] for row in calls])
        result, _ = self.run_synthetic(costs={"compile_wiki_article": 721})
        self.assertEqual(result["status"], "incomplete")

    def test_original_maintenance_creation_does_not_restart_polling_clock(self):
        result, calls = self.run_synthetic(maintenance=True, costs={"create_reprocess_job": 715})
        self.assertEqual(result["status"], "complete")
        self.assertEqual(calls[-1][0:3:2], ("get_reprocess_job_progress", 5.0))
        result, calls = self.run_synthetic(maintenance=True, costs={"create_reprocess_job": 720})
        self.assertEqual(result["status"], "incomplete")
        self.assertNotIn("get_reprocess_job_progress", [row[0] for row in calls])


if __name__ == "__main__":
    unittest.main()
