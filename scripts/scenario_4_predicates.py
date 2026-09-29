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
    sql,
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
    reset_db()

    alice = make_entity("Alice Mentor", space=SPACE, identity_keys=["Alice Mentor"])
    bob = make_entity("Bob Student", space=SPACE, identity_keys=["Bob Student"])
    post(f"/api/v1/spaces/{SPACE}/entities", alice, headers=HEADERS)
    post(f"/api/v1/spaces/{SPACE}/entities", bob, headers=HEADERS)

    doc_text = "Alice Mentor mentors Bob Student in machine learning."
    doc = make_doc("Mentorship", doc_text, "mentors-doc", space=SPACE)
    post(
        f"/api/v1/spaces/{SPACE}/documents",
        {"doc": doc, "source_type": "text/plain"},
        headers=HEADERS,
    )

    def assertion_ready():
        assertions = get(f"/api/v1/spaces/{SPACE}/assertions", headers=HEADERS).json()
        matches = [a for a in assertions if a.get("predicate_raw") == "mentors"]
        if not matches:
            return None
        if any(a.get("subject_entity") is None for a in matches):
            return None
        return matches

    assertions = wait_for(assertion_ready, timeout=180, interval=2)

    def proposal_ready():
        proposals = get("/api/v1/predicates/proposals", headers=HEADERS).json()
        matches = [
            p
            for p in proposals.get("results", [])
            if "mentors" in p.get("aliases", [])
        ]
        return matches[0] if matches else None

    proposal = wait_for(proposal_ready, timeout=180, interval=2)

    approve = post(
        f"/api/v1/predicates/proposals/{proposal['id']}/approve",
        {},
        headers=HEADERS,
    ).json()
    assert approve.get("status") == "active", "approved alias not active"

    aliases = get(
        "/api/v1/predicates/aliases?raw=mentors", headers=HEADERS
    ).json()
    assert aliases.get("results"), "predicate alias not found"
    assert (
        aliases["results"][0].get("status") == "active"
    ), "predicate alias not active"

    def rel_ready():
        rels = sql("SELECT id FROM rel WHERE rel_type = 'mentors';")[0]["result"]
        return rels[0] if rels else None

    wait_for(rel_ready, timeout=180, interval=2)

    # SPEC-002: Verify assertions have canonical predicate_id after predicate approval
    # After predicate approval, assertions should be re-resolved with canonical predicate_id
    def assertions_canonicalized():
        assertions = get(f"/api/v1/spaces/{SPACE}/assertions", headers=HEADERS).json()
        matches = [a for a in assertions if a.get("predicate_raw") == "mentors"]
        if not matches:
            return None
        # All assertions with predicate_raw="mentors" must have predicate_id populated
        if all(a.get("predicate_id") is not None for a in matches):
            return matches
        return None

    canonicalized = wait_for(assertions_canonicalized, timeout=180, interval=2)
    assert canonicalized, "assertions should have canonical predicate_id after approval"
    for assertion in canonicalized:
        assert assertion.get("predicate_id") is not None, f"assertion {assertion['id']} missing predicate_id"
        assert assertion.get("predicate_id") != "", f"assertion {assertion['id']} has empty predicate_id"

def main():
    with scenario_lock():
        run_scenario()


if __name__ == "__main__":
    main()
