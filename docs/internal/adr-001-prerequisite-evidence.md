# ADR-001 Elephant prerequisite evidence

Date: 2026-10-01. Baseline: `1aefaad2de68535ca5b588f58e8ed1ae1bf5b56a`.
Branch: `codex/adr-001-resource-security`.

This change is a prerequisite patch, not implementation completion of ADR-001.
It preserves an explicit empty subject scope and closes ordinary entity
replacement as an implicit relabel operation. No permission string or
authorized declassification operation is introduced.

## Implemented boundary

- JWT-to-policy conversion preserves `None` versus `Some(empty)`. Empty scope
  denies named subjects. The existing subjectless exception still requires
  `handling:subjectless_ok` on the record and in the matching clearance.
- HTTP entity update/upsert/batch and MCP entity update/batch preserve policy,
  level, label set, handling set, subject set and raw subject completeness.
  Same-envelope content updates remain available, including wildcard writers.
- Public replacement compares the observed timestamp, lifecycle and envelope
  at the storage commit. Restoring a deleted batch/upsert is part of that same
  commit. Upsert propagates a failed existing-record read instead of treating
  it as absence. Fresh creation remains a non-overwriting create operation.

## Validation

All Cargo commands use the repository wrapper and isolated worktree outputs.

| Command | Result |
| --- | --- |
| `./scripts/repo-cargo test -p elephant-policy --lib` | 36 passed |
| `./scripts/repo-cargo test -p elephant-mcp -p elephant-api --lib --no-fail-fast entities::` | API 19 passed; MCP 11 passed |
| `./scripts/repo-cargo test -p elephant-mcp -p elephant-api --lib --no-fail-fast` | API 263 tests and MCP 209 tests passed |
| `./scripts/repo-cargo clippy -p elephant-policy -p elephant-storage -p elephant-mcp -p elephant-api --lib --tests -- -D warnings` | Passed |
| `./scripts/repo-cargo fmt --all` | Passed |
| `git diff --check` | Passed |
| `bash scripts/verify_active_surface.sh` | Passed |
| `bash scripts/verify_legacy_terms.sh` | Passed |
| `PATH="/opt/homebrew/bin:$PATH" bash scripts/verify-version-parity.sh` | Passed, workspace version 0.1.0; system Python lacked tomllib |
| `npm ci --prefix scripts/docs --ignore-scripts` | Passed |
| `make docs-check` | Passed including this report: 55 files, 43 pages, 187 JSON examples, 280 local links, no errors |

The first HTTP regression fixture used a nested body and received 422 before
authorization. It was corrected to the endpoint's flat entity contract; both
the focused suite and complete package suites above then passed.
Clippy initially requested `#[must_use]` on `ApiError::forbidden`; the attribute
was added and the same full package command then passed.

| Boundary | Actual evidence |
| --- | --- |
| SurrealDB kv-mem production repository | Same-envelope update succeeds; relabel attempts leave the entire stored record unchanged; denied deleted batch retains the tombstone; valid batch restores; stale authorization after real reclassification returns Conflict and preserves the current record. |
| Embedded production repository | The same cases pass through the actual embedded transaction owner. The stale-envelope test uses a real repository, not the HTTP mock. |
| Surreal native representation | Tests exercise canonical native `deleted_at = NONE` and the serialized `subjects_state = "complete"` in the conditional UPDATE, including deleted timestamp to NONE restoration. They do not establish behavior for manually imported noncanonical NULL/missing state rows or a remote persistent server. |
| HTTP response | Router tests deny ordinary relabel attempts across update/upsert/batch/deleted batch and compare the mock sink before/after. An empty subject scope returns 403 without the private body canary. The HTTP mock is not evidence of atomicity. |
| Authenticated MCP response | A real loopback HTTP MCP server verifies a signed JWT, uses embedded storage, and denies the private body canary for `Some(empty)`. The otherwise equivalent unrestricted token receives it. |
| Subject exception semantics | Complete/partial/unknown metadata, named/empty subjects, waiver present/absent and clearance present/absent are exercised. Known subjects never disappear through the waiver. |

## Open scope

- A separately authorized relabel/declassification operation remains OPEN.
  Ordinary-write refusal does not satisfy the full authorized release contract.
- Knowledge-view definition mutation, identity merge, other domain mutation
  owners, trusted pipeline writers and generic internal repository updates
  retain their existing contracts and require separate integration.
- Authenticated requester/issuer federation, actor chains, live revocation,
  generated owner participation, protected-read/use/disclosure receipts,
  dependency epochs and recovery custody are not implemented by this patch.
- No hosted CI, adversarial project acceptance, deployment, release, push or
  PR has been performed for this checkpoint. Repository-wide pre-PR gates
  remain the responsibility of the eventual integrated candidate.

## Checkpoint file ownership

`bin/elephant-api/src/authz.rs`, `bin/elephant-api/src/mcp.rs`,
`bin/elephant-api/src/routes/entities.rs`,
`bin/elephant-api/src/routes/entities_test.rs`,
`crates/mcp/src/handlers/entities.rs`, `crates/policy/src/lib.rs`,
`crates/policy/src/update.rs`, `crates/storage/src/entity_repository.rs`,
`docs/active-architecture.md`, `docs/concepts/security.mdx`, and this file.
