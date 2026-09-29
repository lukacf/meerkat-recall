import os
import subprocess

from phase13_helpers import (
    auth_headers,
    get,
    make_claims,
    make_source,
    normalize_record_id,
    post,
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

    # Verify we start clean
    def check_clean_state():
        entities = get(f"/api/v1/spaces/{SPACE}/entities", headers=HEADERS).json()
        return len(entities) == 0

    try:
        wait_for(check_clean_state, timeout=10, interval=1)
    except TimeoutError:
        print("Warning: Database not empty after reset!")

    email_contacts = [
        {"name": "Alice Adams", "email": "alice@example.com"},
        {"name": "Bob Brown", "email": "bob@example.com"},
        {"name": "Carla Cruz", "email": "carla@example.com"},
        {"name": "David Doe", "email": "david@example.com"},
        {"name": "Eva Evans", "email": "eva@example.com"},
        {"name": "Frank Frost", "email": "frank@example.com"},
        {"name": "Grace Green", "email": "grace@example.com"},
        {"name": "Hank Hill", "email": "hank@example.com"},
        {"name": "Ivy Indigo", "email": "ivy@example.com"},
        {"name": "Jack Jade", "email": "jack@example.com"},
    ]

    for contact in email_contacts:
        source = make_source(
            "email",
            contact["email"],
            space=SPACE,
            metadata={"contact": contact},
        )
        post(f"/api/v1/spaces/{SPACE}/sources", source, headers=HEADERS)

    # Wait for entities to appear (async ingestion)
    def check_email_entities():
        entities = get(f"/api/v1/spaces/{SPACE}/entities", headers=HEADERS).json()
        if len(entities) == 10:
            return entities
        return None

    wait_for(check_email_entities, timeout=30, interval=1, error_message="expected 10 entities after email import")

    linkedin_contacts = [
        {
            "name": "Alice Adams",
            "email": "alice@example.com",
            "profile": "https://linkedin.com/in/alice",
            "company": "Acme Corp",
            "job_title": "Engineer",
        },
        {
            "name": "Bob Brown",
            "email": "bob@example.com",
            "profile": "https://linkedin.com/in/bob",
        },
        {
            "name": "Carla Cruz",
            "email": "carla@example.com",
            "profile": "https://linkedin.com/in/carla",
        },
        {
            "name": "David Doe",
            "email": "david@example.com",
            "profile": "https://linkedin.com/in/david",
        },
        {
            "name": "Eva Evans",
            "email": "eva@example.com",
            "profile": "https://linkedin.com/in/eva",
        },
        {
            "name": "Liam Lake",
            "email": "liam@example.com",
            "profile": "https://linkedin.com/in/liam",
        },
        {
            "name": "Mona Moss",
            "email": "mona@example.com",
            "profile": "https://linkedin.com/in/mona",
        },
        {
            "name": "Nate North",
            "email": "nate@example.com",
            "profile": "https://linkedin.com/in/nate",
        },
    ]

    for contact in linkedin_contacts:
        content = contact.get("profile") or contact["email"]
        source = make_source(
            "web",
            content,
            space=SPACE,
            metadata={"contact": contact},
        )
        post(f"/api/v1/spaces/{SPACE}/sources", source, headers=HEADERS)

    # Wait for merge to complete
    def check_merged_entities():
        entities = get(f"/api/v1/spaces/{SPACE}/entities", headers=HEADERS).json()
        if len(entities) == 13:
            return entities
        return None

    entities = wait_for(check_merged_entities, timeout=30, interval=1, error_message="expected 13 entities after merge")

    overlap_email = "alice@example.com"
    overlap_profile = "https://linkedin.com/in/alice"
    merged = next(
        (
            entity
            for entity in entities
            if overlap_email in entity.get("identity_keys", [])
        ),
        None,
    )
    assert merged is not None, "merged entity not found"
    assert overlap_profile in merged.get(
        "identity_keys", []
    ), "merged entity missing LinkedIn identity key"

    all_entities = get(
        f"/api/v1/spaces/{SPACE}/entities?include_all=true", headers=HEADERS
    ).json()
    company = next(
        (entity for entity in all_entities if entity.get("display_name") == "Acme Corp"),
        None,
    )
    assert company is not None, "company entity missing"

    rels = sql("SELECT * FROM rel WHERE rel_type = 'works_at';")[0]["result"]
    assert any(
        normalize_record_id(rel.get("in")) == merged["id"]
        and normalize_record_id(rel.get("out")) == company["id"]
        for rel in rels
    ), "works_at relationship not found"

def main():
    with scenario_lock():
        run_scenario()


if __name__ == "__main__":
    main()
