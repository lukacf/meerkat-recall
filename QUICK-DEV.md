# Quick development reference

The maintained workflow is in [DEV.md](DEV.md). From the repository root:

```bash
docker compose -f docker-compose.dev.yml up -d
source .envrc
./scripts/repo-cargo run -p meerkat-recall-bin -- migrate
./scripts/repo-cargo run -p meerkat-recall-bin -- serve
```

Use [CONTRIBUTING.md](CONTRIBUTING.md) for tests and review requirements.
Use `./rebuild-fast.sh` only when iterating on the local Docker example.
