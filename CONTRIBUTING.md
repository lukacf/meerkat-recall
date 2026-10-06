# Contributing

Thanks for helping make Meerkat Recall better.

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
docker compose run --rm elephant meerkat-recall migrate
docker compose up -d elephant
```

For embedded local mode:

```bash
SKIP_AUTH=true ELEPHANT_HOST=127.0.0.1 ELEPHANT_DB=elephant-db \
  ./scripts/repo-cargo run -p meerkat-recall-bin -- serve
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

Validate the documentation site with Node.js 22 or newer:

```bash
npm ci --prefix scripts/docs --ignore-scripts
make docs-check
```

This compiles MDX and checks page titles, navigation coverage, local links and
heading anchors, JSON examples, and MCP tool/profile coverage against Rust
source. Use Markdown links/images in `.md` files and quoted `href`/`src` strings
in MDX components so link targets can be checked. CI runs the same check. This
does not validate live API schemas or Mintlify's component registry. To inspect
layout, use Node.js 22 and run the pinned Mintlify preview:

```bash
cd docs
npx mint@4.2.813 dev --port 3333
```

Open `http://localhost:3333` and follow the quickstart and changed pages in a
browser. Check diagrams, Steps/Tabs, table widths, and direct heading links.

Write concepts to explain the model, guides to complete a task, and references
to describe exact contracts. Guides should state prerequisites, distinguish
example IDs from returned IDs, show how to verify the outcome, and explain the
next diagnostic step when work fails. Keep examples fictional and use ASCII
hyphens.

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
