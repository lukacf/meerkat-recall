import os
import subprocess
import time

from phase13_helpers import (
    auth_headers,
    make_claims,
    make_doc,
    post,
    get,
    scenario_lock,
    wait_for,
    wait_for_api_ready,
)

SPACE = os.getenv("SPACE", "default")
HEADERS = auth_headers(make_claims("phase13-admin", SPACE))


def reset_db():
    if os.getenv("SKIP_RESET") in ("1", "true", "yes"):
        wait_for_api_ready(timeout=180)
        return
    subprocess.run(["scripts/reset_test_db.sh", "--ensure-services"], check=True)
    wait_for_api_ready(timeout=180)


def run_scenario():
    if os.getenv("PIPELINE_BUDGET_DAILY_ITEMS_GRAPH") != "5":
        raise RuntimeError("PIPELINE_BUDGET_DAILY_ITEMS_GRAPH must be set to 5")

    reset_db()

    for idx in range(10):
        text = f"Budget test document {idx}."
        doc = make_doc(f"Budget Doc {idx}", text, f"budget-doc-{idx}", space=SPACE)
        post(
            f"/api/v1/spaces/{SPACE}/documents",
            {"doc": doc, "source_type": "text/plain"},
            headers=HEADERS,
        )

    def completed_extracts():
        items = get(
            "/api/v1/work_items",
            params={"status": "done", "kind": "extract_assertions"},
            headers=HEADERS,
        ).json()
        done = items.get("results", [])
        if len(done) >= 5:
            return len(done)
        return None

    done_count = wait_for(completed_extracts, timeout=180, interval=2)
    assert done_count == 5, f"expected 5 completed extracts, got {done_count}"

    runnable = get(
        "/api/v1/work_items",
        params={"status": "runnable", "kind": "extract_assertions"},
        headers=HEADERS,
    ).json()
    pending_extracts = runnable.get("results", [])
    assert (
        len(pending_extracts) == 5
    ), f"expected 5 pending extracts, got {len(pending_extracts)}"

    budget = get("/api/v1/budget/today", headers=HEADERS).json()
    assert budget.get("items_graph") == 5, f"expected 5 items, got {budget.get('items_graph')}"

    # Verify the cap persists, not just the first batch.
    time.sleep(30)
    done_after = completed_extracts()
    assert done_after == 5, f"expected extracts to remain capped at 5, got {done_after}"

    runnable_after = get(
        "/api/v1/work_items",
        params={"status": "runnable", "kind": "extract_assertions"},
        headers=HEADERS,
    ).json()
    pending_after = runnable_after.get("results", [])
    assert (
        len(pending_after) == 5
    ), f"expected extracts to remain capped with 5 pending, got {len(pending_after)}"

    budget_after = get("/api/v1/budget/today", headers=HEADERS).json()
    assert (
        budget_after.get("items_graph") == 5
    ), f"expected budget items to remain 5, got {budget_after.get('items_graph')}"

def main():
    with scenario_lock():
        run_scenario()


if __name__ == "__main__":
    main()
