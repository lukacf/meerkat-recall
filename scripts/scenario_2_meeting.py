import os
import subprocess

from phase13_helpers import (
    auth_headers,
    make_claims,
    make_doc,
    make_entity,
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
        return
    subprocess.run(["scripts/reset_test_db.sh", "--ensure-services"], check=True)


def run_scenario():
    reset_db()
    wait_for_api_ready(timeout=180)

    attendees = ["Alice Johnson", "Bob Smith", "Carol Diaz"]
    for name in attendees:
        entity = make_entity(name, space=SPACE, identity_keys=[name])
        post(f"/api/v1/spaces/{SPACE}/entities", entity, headers=HEADERS)

    notes = """Meeting Notes

Attendees: Alice Johnson, Bob Smith, Carol Diaz

Decisions:
- Adopt Q1 OKRs
- Use vendor XYZ for hosting

Action Items:
- Alice Johnson: Draft proposal by 2025-02-15
- Bob Smith: Prepare budget due 2025-02-20
"""

    os.makedirs("tmp", exist_ok=True)
    notes_path = os.path.join("tmp", "phase13_meeting_notes.md")
    with open(notes_path, "w", encoding="utf-8") as handle:
        handle.write(notes)

    doc = make_doc("Team Meeting", notes, "meeting-notes", space=SPACE)
    resp = post(
        f"/api/v1/spaces/{SPACE}/documents",
        {"doc": doc, "source_type": "text/markdown"},
        headers=HEADERS,
    )
    payload = resp.json()
    assert payload.get("revision_id"), "missing revision_id in response"

    def extraction_done():
        items = get("/api/v1/work_items?status=done", headers=HEADERS).json()
        if any(item.get("kind") == "extract_assertions" for item in items.get("results", [])):
            return items
        return None

    wait_for(extraction_done, timeout=180, interval=2)

    def assertions_ready():
        assertions = get(f"/api/v1/spaces/{SPACE}/assertions", headers=HEADERS).json()
        if len(assertions) < 7:
            return None
        attendee_assertions = [
            a for a in assertions if a.get("predicate_raw") == "attended"
        ]
        if len(attendee_assertions) < 3:
            return None
        if any(a.get("subject_entity") is None for a in attendee_assertions):
            return None
        action_due = [
            a for a in assertions if a.get("predicate_raw") == "action item due"
        ]
        if len(action_due) < 2:
            return None
        if any(a.get("validity_start") is None for a in action_due):
            return None
        return assertions

    wait_for(assertions_ready, timeout=180, interval=2)


def main():
    with scenario_lock():
        run_scenario()


if __name__ == "__main__":
    main()
