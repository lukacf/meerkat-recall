# Local development

Use the unified `elephant` binary. The default `serve` command runs HTTP, MCP
and managed workers in one process. See [CONTRIBUTING.md](CONTRIBUTING.md) for
verification gates and [active architecture](docs/active-architecture.md) for
runtime ownership.

Start a local SurrealDB and use the development environment:

```bash
docker compose -f docker-compose.dev.yml up -d
source .envrc
./scripts/repo-cargo run -p meerkat-recall-bin -- migrate
./scripts/repo-cargo run -p meerkat-recall-bin -- serve
```

These defaults bind to loopback and bypass authentication for local development.
Set provider credentials in your shell when exercising model-backed extraction
or embeddings. Do not commit credentials or private input documents.

After a code change, stop the running process and rerun the command. Cargo
reuses the isolated worktree build cache. Build time depends on the machine,
changed crates and cache state.

```bash
make fmt-check
make verify-surface
make verify-version-parity
make lint
make test-unit
```

For the local container example, stop the local process and development database
before starting the other Compose stack (both use port 8000):

```bash
docker compose -f docker-compose.dev.yml down
docker compose up -d surrealdb
docker compose run --rm elephant meerkat-recall migrate
docker compose up --build -d elephant
```

`./rebuild-fast.sh` rebuilds the `elephant` service and recreates it without
pruning shared Docker caches. `--full` disables build-layer reuse for that build.
The Compose files are development examples, not production deployment manifests.
