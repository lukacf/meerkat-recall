import os
import subprocess

from phase13_helpers import (
    auth_headers,
    make_claims,
    make_doc,
    make_entity,
    post,
    get,
    sql,
    wait_for,
    scenario_lock,
    ensure_predicate_alias,
    normalize_record_id,
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
    reset_db()

    ensure_predicate_alias("has title", HEADERS, description="Person title")

    bob = make_entity("Bob Smith", space=SPACE, identity_keys=["Bob Smith"])
    bob_resp = post(f"/api/v1/spaces/{SPACE}/entities", bob, headers=HEADERS).json()
    bob_id = bob_resp["id"]

    doc1_text = "Bob Smith is VP Engineering."
    doc1 = make_doc("Org Chart", doc1_text, "org-chart", space=SPACE)
    post(
        f"/api/v1/spaces/{SPACE}/documents",
        {"doc": doc1, "source_type": "text/plain"},
        headers=HEADERS,
    )

    def truth_slot_exists():
        rows = sql("SELECT * FROM truth_slot WHERE predicate_id = 'has title';")[0]["result"]
        for row in rows:
            if normalize_record_id(row.get("subject_entity")) == bob_id:
                return row
        return None

    slot = wait_for(truth_slot_exists, timeout=180, interval=2)

    doc2_text = "Bob Smith is CTO."
    doc2 = make_doc("Press Release", doc2_text, "press-release", space=SPACE)
    post(
        f"/api/v1/spaces/{SPACE}/documents",
        {"doc": doc2, "source_type": "text/plain"},
        headers=HEADERS,
    )

    # Helper: Ensure extraction actually happened for doc2
    def extraction_completed():
        work_items = get(f"/api/v1/work_items?status=completed&limit=10", headers=HEADERS).json()
        # Look for work item related to doc2 (we don't have its ID easily, but we can check count or content if needed)
        # For now, just ensuring *some* work items complete is a good liveness check
        if len(work_items) > 0:
             return True
        return None

    try:
        wait_for(extraction_completed, timeout=60, interval=2)
    except TimeoutError:
        print("Warning: No completed work items found. Pipeline might be stalled.")

    def conflict_group_exists():
        slots = sql("SELECT * FROM truth_slot WHERE predicate_id = 'has title';")[0]["result"]
        slot = next(
            (
                row
                for row in slots
                if normalize_record_id(row.get("subject_entity")) == bob_id
            ),
            None,
        )
        if slot is None:
            return None
        if slot.get("conflict_group"):
            return slot
        groups = sql(f"SELECT * FROM conflict_group WHERE slot_key = '{slot['slot_key']}';")[
            0
        ]["result"]
        return groups[0] if groups else None

    slot = wait_for(conflict_group_exists, timeout=180, interval=2)

    def resolved_title():
        facts = get(
            f"/api/v1/spaces/{SPACE}/entities/{bob_id}/facts", headers=HEADERS
        ).json()
        items = facts.get("facts", [])
        title_facts = [fact for fact in items if fact.get("predicate_id") == "has title"]
        if len(title_facts) != 1:
            return None
        value = title_facts[0].get("value", {})
        if isinstance(value, dict) and value.get("type") == "string" and value.get("value") == "CTO":
            return facts
        return None

    facts = wait_for(resolved_title, timeout=180, interval=2)

    conflict_group = sql(
        f"SELECT * FROM conflict_group WHERE slot_key = '{slot['slot_key']}';"
    )[0]["result"]
    assert conflict_group, "conflict_group not found"
    assert (
        len(conflict_group[0].get("claim_ids", [])) >= 2
    ), "conflict_group missing competing claims"

def main():
    with scenario_lock():
        run_scenario()


if __name__ == "__main__":
    main()
