# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Meerkat Recall is a unified knowledge system built in Rust using SurrealDB and the embedded `elephant-db` runtime. It provides entity management, relationship traversal, identity resolution, assertion storage, source-first document pipelines, truth maintenance, and ABAC-based policy enforcement.

## Build & Development Commands

Use `./scripts/repo-cargo` for Cargo commands so concurrent worktrees do not
share build outputs. The standard validation commands are:

```bash
make fmt-check
make verify-surface
make verify-version-parity
make lint
make test-unit

./scripts/repo-cargo test -p elephant-pipeline test_name
./scripts/repo-cargo run -p meerkat-recall-bin -- serve
```

Read `docs/active-architecture.md` and `docs/non-active-catalog.md` before editing
runtime surfaces. `DEV.md` covers loopback development configuration. Heavy and
live provider tests are opt-in; see `CONTRIBUTING.md`.

## Workspace Structure

**Binaries** (`bin/`):
- `elephant` (`meerkat-recall-bin`) - Primary CLI: serve, worker, migrate, status, snapshot, data-migrate and demo-wiki
- `elephant-api` - HTTP API server (Axum, port 3000)
- `elephant-pipeline` - Background worker runtime for ingest, embedding, maintenance, and truth maintenance

`bin/elephant-supervisor` and `bin/elephant-cli` are excluded legacy scaffolds.

**Core Crates** (`crates/`):
- `elephant-db` - Embedded in-memory graph/vector storage kernel
- `types` - Domain types, DTOs, record IDs, validation
- `storage` - SurrealDB repositories and connection pooling
- `policy` - ABAC policy engine and redaction
- `identity` - Identity key management and merge strategy
- `pipeline` - Scheduler, ingestion, embedding, maintenance, and truth maintenance workers
- `embeddings` - Vector provider abstraction
- `mcp` - MCP protocol handlers and tool registry
- `outbox` - Event outbox for reliable messaging
- `extensions` - Extension manifest and reference handling
- `migrations` - SurrealQL migration runner

**Tests** (`tests/`):
- `scenarios/` - E2E scenario tests (S1.x-S4.x, M1-M10)
- `e2e/` - ignored end-to-end tests and feature-gated scenarios
- `benchmarks/` - benchmark harnesses
- `dev_readiness/` - Development readiness verification

## Critical SurrealDB Patterns

These patterns prevent SurrealDB driver integration bugs at the storage boundary:

### DB Proxy Structs
Do NOT use `#[serde(flatten)]` at the database boundary. Use separate storage representation structs:

```rust
// Storage proxy (clean, no flatten)
#[derive(Deserialize)]
struct EntityDb {
    id: RecordId,
    space_id: String,
    created_at: DateTime<Utc>,
}

// Convert to domain model
impl TryFrom<EntityDb> for Entity { ... }
```

### Manual Visitors
Never use `serde_json::Value` as intermediate in custom deserializers. Support both string and enum shapes via manual `Visitor` implementations.

### No Silent Defaults
Never use `.unwrap_or_else(Utc::now)` or similar fallbacks. Use `TryFrom` with explicit `StorageError::DataIntegrityError` when mandatory fields are missing.

### Raw Projections
Do NOT use `time::format` in SurrealQL projections for storage proxies. Return raw native types and validate on conversion.

## Testing Tiers

1. **Unit tests** - Domain logic in isolation (`#[cfg(test)]` modules)
2. **Integration tests** - Use `kv-mem` SurrealDB backend
3. **Scenarios** - System-level E2E tests verifying invariants

## Workspace Lints

The workspace enforces strict lints:
- `unsafe_code = "forbid"`
- `unwrap_used`, `expect_used`, `panic`, `todo` = "warn"
- Clippy `pedantic` and `nursery` enabled

## Environment Variables

Key variables (see docker-compose.yml):
- `SURREALDB_URL`, `SURREALDB_NAMESPACE`, `SURREALDB_DATABASE`
- `ELEPHANT_HOST`, `ELEPHANT_PORT`
- `RUST_LOG` (default: `info,elephant=debug`)
- `SKIP_AUTH` (development only)
