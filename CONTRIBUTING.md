# Contributing

Thanks for helping make Elephant better.

## Development Setup

Install Rust 1.94 or newer. Docker Compose is optional for the persistent local
stack; the in-memory demo needs no database server or model credentials.

Use the repository Cargo wrapper so local builds do not share lock state with
other checkouts:

```bash
./scripts/repo-cargo check --workspace --all-targets --all-features
```

For the local SurrealDB-backed stack:

```bash
docker compose up -d surrealdb
docker compose run --rm elephant elephant migrate
docker compose up -d elephant
```

For embedded local mode:

```bash
SKIP_AUTH=true ELEPHANT_HOST=127.0.0.1 ELEPHANT_DB=elephant-db \
  ./scripts/repo-cargo run -p elephant-bin -- serve
```

## Before Opening A Pull Request

Run the same gates used by the release workflow:

```bash
./scripts/repo-cargo fmt --all
./scripts/repo-cargo clippy --workspace --all-targets --all-features -- -D warnings
./scripts/repo-cargo test --workspace --lib --bins
bash scripts/verify_active_surface.sh
bash scripts/verify_legacy_terms.sh
```

When changing active runtime behavior, update `docs/active-architecture.md` and
keep `docs/non-active-catalog.md` aligned with any planned or removed surfaces.
Do not document removed work kinds as active runtime behavior; keep planned
scaffolds clearly marked as non-active.

## Documentation Changes

- Keep top-level docs public-facing: avoid local machine paths, private hosts,
  unpublished roadmap commitments, and maintainer-only workflow details.
- Treat `docs/active-architecture.md` as the active runtime source of truth.
- Use `docs/non-active-catalog.md` for removed legacy behavior and planned
  scaffolds that are not dispatched by the runtime.

## Code Style

- Follow the existing crate/module patterns before adding new abstractions.
- Keep SurrealDB storage boundaries explicit: no `#[serde(flatten)]` on DB proxy
  structs, no `serde_json::Value` as a custom-deserializer midpoint, and no
  silent defaults for mandatory fields.
- Preserve `space_id` isolation and ABAC-aware reads/writes.
- Add focused tests for behavior changes and broader tests for shared contracts.

## Security

Please do not file public issues for unpatched vulnerabilities. See
`SECURITY.md` for reporting guidance.
