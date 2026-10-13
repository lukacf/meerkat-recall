# E2E-required scenarios

These scenarios are **intentionally excluded** from the default fast test run because they
exercise real storage/migrations and multi-component flows.

## Elephant API (DB-backed)

- **Docs ingest + embeddings + vector search path** — `tests/e2e/api/docs_test.rs`
- **Identity merge + identity key repointing** — `tests/e2e/api/identity_test.rs`

## Workspace scenarios (full pipeline)

- **Scenario suite** — `tests/e2e/scenarios/*` (uses `StorageClient` + migrations and runs multi-stage flows)
