"""Offline regression of the ACTUAL legacy shell evaluator, never a product run.

Functions are sourced without main. All network/model/service functions are
replaced with explicit test spies. Results are evaluator evidence only.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

RUNNER = Path(__file__).resolve().parents[1] / "golden_scenarios/run_golden_scenarios.sh"


@unittest.skipUnless(shutil.which("bash") and shutil.which("jq"), "offline runner tests require bash and jq")
class GoldenRunnerTests(unittest.TestCase):
    def run_case(self, score="0.1", verdict="fail", docs="1", mode="acceptance", enabled="true"):
        with tempfile.TemporaryDirectory(prefix="semantic-golden-oracle-") as directory:
            env = {**os.environ, "RUNNER_UNDER_TEST": str(RUNNER), "ORACLE_TMP": directory,
                   "SPY_SCORE": score, "SPY_VERDICT": verdict, "SPY_DOCS": docs,
                   "SPY_MODE": mode, "SPY_ENABLED": enabled}
            script = r'''
source "$RUNNER_UNDER_TEST"
RUN_DIR="$ORACLE_TMP"
RESULTS_JSONL="$ORACLE_TMP/results.jsonl"
RUN_MODE="$SPY_MODE"
WAIT_FOR_EMBEDDINGS=true
api_post_json() {
  printf '%s\t%s\n' "$2" "$(jq -c . <<<"$3")" >> "$ORACLE_TMP/requests.txt"
  case "$2" in
    /v1/docs/search) jq -n --argjson total "$SPY_DOCS" '{results:[],total:$total}' > "$4" ;;
    *) printf '{"results":[],"total":100}' > "$4" ;;
  esac
  printf '200'
}
api_get() { printf '{"results":[],"total":0}' > "$3"; printf '200'; }
run_llm_judge() {
  jq -n --argjson score "$SPY_SCORE" --arg verdict "$SPY_VERDICT" --argjson enabled "$SPY_ENABLED" \
    '{enabled:$enabled,overall_score:$score,verdict:$verdict,summary:"EVALUATOR SPY ONLY",issues:[]}' > "$7"
}
run_scenario '{"id":"original-case","name":"Original","complexity":1,"query":"Where is the red lever?","min_assertions":1,"min_events":1,"ingestions":[]}'
'''
            proc = subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True, timeout=15)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            result = json.loads(Path(directory, "results.jsonl").read_text())
            requests = Path(directory, "requests.txt").read_text().splitlines()
            return result, requests

    def test_below_threshold_never_becomes_advisory_with_high_graph_counts(self):
        result, _ = self.run_case()
        self.assertEqual(result["status"], "fail")
        self.assertFalse(result["semantic_acceptance"])
        self.assertTrue(any("overall_score" in issue for issue in result["issues"]))

    def test_fail_verdict_cannot_be_overruled_by_high_score(self):
        result, _ = self.run_case(score="0.99", verdict="fail")
        self.assertEqual(result["status"], "fail")

    def test_empty_original_retrieval_cannot_borrow_stored_graph(self):
        result, _ = self.run_case(score="0.99", verdict="pass", docs="0")
        self.assertEqual(result["status"], "fail")
        self.assertTrue(any("original query" in issue for issue in result["issues"]))

    def test_exploratory_output_never_marks_acceptance(self):
        result, _ = self.run_case(score="0.99", verdict="pass", mode="exploratory")
        self.assertEqual(result["status"], "exploratory_observation")
        self.assertFalse(result["semantic_acceptance"])

    def test_disabled_judge_is_not_pass(self):
        result, _ = self.run_case(score="null", verdict="disabled", enabled="false")
        self.assertEqual(result["status"], "fail")

    def test_original_queries_are_not_rewritten(self):
        _, requests = self.run_case()
        queries = [json.loads(line.split("\t", 1)[1]).get("query") for line in requests]
        self.assertEqual([q for q in queries if q is not None],
                         ["Where is the red lever?", "Where is the red lever?", "Alex Rivera"])

    def test_no_network_main_runs_on_source(self):
        # Function-only import is the actual shell path used by these tests.
        # Any future unguarded main/network invocation fails immediately.
        env = {**os.environ, "RUNNER_UNDER_TEST": str(RUNNER)}
        script = 'curl() { exit 91; }; docker() { exit 92; }; source "$RUNNER_UNDER_TEST"; declare -F run_scenario >/dev/null'
        result = subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_judge_payload_excludes_unfiltered_evidence_and_ingestion_hints(self):
        with tempfile.TemporaryDirectory(prefix="semantic-golden-judge-") as directory:
            env = {**os.environ, "RUNNER_UNDER_TEST": str(RUNNER), "ORACLE_TMP": directory}
            script = r'''
source "$RUNNER_UNDER_TEST"
RUN_DIR="$ORACLE_TMP"
OPENAI_API_KEY=TEST_ONLY_NEVER_TRANSMITTED
JUDGE_ENABLED=true
printf '{"results":[]}' > "$ORACLE_TMP/docs.json"
printf '{"results":[]}' > "$ORACLE_TMP/events.json"
curl() {
  local output
  while (( $# )); do
    if [[ "$1" == "-o" ]]; then output="$2"; shift 2; else shift; fi
  done
  jq -n '{choices:[{message:{content:"{\"overall_score\":0.1,\"relevance_score\":0.1,\"grounding_score\":0.1,\"temporal_score\":0.1,\"contradiction_score\":0.1,\"verdict\":\"fail\"}"}}]}' > "$output"
  printf '200'
}
run_llm_judge q '{"id":"q","name":"q","complexity":1,"min_assertions":1,"min_events":1,"ingestions":[{"note":"HIDDEN EXPECTED ANSWER"}]}' space 'original query' "$ORACLE_TMP/events.json" "$ORACLE_TMP/docs.json" "$ORACLE_TMP/judge.json"
'''
            proc = subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True, timeout=10)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            payload = json.loads(Path(directory, "q_judge_input.json").read_text())
            self.assertEqual(set(payload["retrieval"]), {"docs", "events"})
            self.assertNotIn("ingestions", payload["scenario"])
            self.assertNotIn("HIDDEN EXPECTED ANSWER", json.dumps(payload))


if __name__ == "__main__":
    unittest.main()
