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

    ensure_predicate_alias("works_at", HEADERS, description="Employment", alias="works at")
    ensure_predicate_alias("has title", HEADERS, description="Job title")

    acme = make_entity("Acme Corp", entity_type="org", space=SPACE, identity_keys=["Acme Corp"])
    acme_resp = post(
        f"/api/v1/spaces/{SPACE}/entities", acme, headers=HEADERS
    ).json()
    acme_id = acme_resp["id"]

    jane = make_entity(
        "Dr. Jane Smith",
        space=SPACE,
        identity_keys=["Dr. Jane Smith"],
    )
    jane_resp = post(
        f"/api/v1/spaces/{SPACE}/entities", jane, headers=HEADERS
    ).json()
    jane_id = jane_resp["id"]

    bio = "Dr. Jane Smith joined Acme Corp as Chief Scientist in March 2024."
    doc = make_doc("Bio", bio, "jane-bio", space=SPACE)
    post(
        f"/api/v1/spaces/{SPACE}/documents",
        {"doc": doc, "source_type": "text/plain"},
        headers=HEADERS,
    )

    def assertions_ready():
        assertions = get(f"/api/v1/spaces/{SPACE}/assertions", headers=HEADERS).json()
        if len(assertions) < 2:
            return None
        works = [a for a in assertions if a.get("predicate_raw") == "works at"]
        titles = [a for a in assertions if a.get("predicate_raw") == "has title"]
        if not works or not titles:
            return None
        if works[0].get("subject_entity") is None or works[0].get("object_entity") is None:
            return None
        if titles[0].get("subject_entity") is None:
            return None
        return assertions

    wait_for(assertions_ready, timeout=180, interval=2)

    def rel_ready():
        rels = sql("SELECT * FROM rel WHERE rel_type = 'works_at';")[0]["result"]
        for rel in rels:
            if (
                normalize_record_id(rel.get("in")) == jane_id
                and normalize_record_id(rel.get("out")) == acme_id
            ):
                return rel
        return None

    wait_for(rel_ready, timeout=180, interval=2)

    def attr_ready():
        attrs = sql("SELECT * FROM attribute WHERE attr_type = 'has title';")[0]["result"]
        for attr in attrs:
            if normalize_record_id(attr.get("subject")) == jane_id:
                return attr
        return None

    wait_for(attr_ready, timeout=180, interval=2)

    def truth_ready():
        slots = sql(
            "SELECT * FROM truth_slot WHERE predicate_id IN ['works_at', 'has title'];"
        )[0]["result"]
        matches = [
            slot
            for slot in slots
            if normalize_record_id(slot.get("subject_entity")) == jane_id
        ]
        return matches if len(matches) >= 2 else None

    wait_for(truth_ready, timeout=180, interval=2)

    works_at = sql("SELECT * FROM rel WHERE rel_type = 'works_at';")[0]["result"]
    assert any(
        normalize_record_id(row.get("in")) == jane_id
        and normalize_record_id(row.get("out")) == acme_id
        for row in works_at
    ), "Jane not linked to Acme"

    facts = get(
        f"/api/v1/spaces/{SPACE}/entities/{jane_id}/facts", headers=HEADERS
    ).json()
    titles = [fact for fact in facts.get("facts", []) if fact.get("predicate_id") == "has title"]
    assert titles, "title fact missing"
    assert (
        "Chief Scientist" in str(titles[0].get("value"))
    ), "title fact incorrect"

def main():
    with scenario_lock():
        run_scenario()


if __name__ == "__main__":
    main()
