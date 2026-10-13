# Local development

Use the unified `elephant` binary. The default `serve` command runs HTTP, MCP
and managed workers in one process. See [CONTRIBUTING.md](CONTRIBUTING.md) for
verification gates and [active architecture](docs/active-architecture.md) for
runtime ownership.

Both Compose examples require a locally available **paired Recall SurrealDB
image** built from the same reviewed core/SDK/KV sources as Recall. Follow the
[source preparation and image recipe](packaging/recall-surreal/README.md), record
its image identity, and set `RECALL_SURREAL_IMAGE` to that exact local tag or
digest. There is no vanilla fallback or published-image claim. The examples
use `pull_policy: never`, so starting them does not fetch a database image.

Run from this checkout, then start the paired database and use the development
environment:

```bash
# Set this to your already built and loaded paired image:
export RECALL_SURREAL_IMAGE=recall-surreal:REVIEWED_SOURCE
docker compose -f docker-compose.dev.yml up -d --wait
source .envrc
./scripts/repo-cargo run -p meerkat-recall-bin -- migrate
./scripts/repo-cargo run -p meerkat-recall-bin -- serve
```

These defaults bind to loopback and bypass authentication for local development.
The source environment retains the original journal in
`.elephant/dev-recovery` and managed backups in `.elephant/dev-backups`.
Keep these paths stable across explicit `migrate`, `serve` and restore; a fresh
journal is not a replacement for a lost original. A restored backup uses the
same original journal. The paired database health check is only process
availability; Recall still verifies the native capability, schema receipts and
recovery barrier before readiness.

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
docker compose up -d --wait surrealdb
docker compose run --build --rm elephant meerkat-recall migrate
docker compose up -d elephant
```

The container stack retains database files in `surrealdb_data` and Recall's
journal/backups in the separate `recall_state` volume. Both the one-shot
migration and the long-running server use the same Recall volume. Stop the
server before running migrations; remote clients share an exclusive journal
lock. Ordinary `docker compose down` retains both volumes. Deleting the original
journal while retaining a database or backup prevents safe restore; do not use
volume deletion as a schema or recovery repair.

`./rebuild-fast.sh` rebuilds the `elephant` service and recreates it without
pruning shared Docker caches. `--full` disables build-layer reuse for that build.
The Compose files are development examples, not production deployment manifests.
