# Authenticated Compose installation

Use the release's **published digest references** for `ghcr.io/lukacf/meerkat-recall`
and `ghcr.io/lukacf/recall-surreal`. Both images currently target Linux amd64.
An Apple Silicon Docker host runs this platform through emulation; that is
separate from the native macOS artifact and its inference qualification.

From a Recall checkout, create a new installation without starting anything:

```sh
scripts/release/prepare-compose --recall-image "$RECALL_IMAGE_BY_DIGEST" \
  --paired-image "$PAIRED_IMAGE_BY_DIGEST" --output /absolute/private/recall
cd /absolute/private/recall
docker compose config --quiet
docker compose pull
docker compose up -d recall
```

The preparer creates random, distinct database and signing secrets with mode
0600 under a mode 0700 directory. Back up that directory securely. `init` copies
them into separate named volumes with the service UIDs, refuses changed existing
keys, and initializes writable data directories. Database readiness precedes
schema migration; a successful migration precedes Recall. Normal startup runs
the API and managed workers in one container. Provider/space configuration and
grants still require explicit setup; startup does not approve any provider.

The API is reachable at `http://127.0.0.1:3000`; the database has **no host port**.
For remote access, terminate TLS in your reverse proxy and explicitly configure
the API's allowed hosts/origins. The database and Recall communicate over their
Compose network. Recall retains outbound access for explicitly configured
providers. Images run as UID10001 (Recall) and UID65532 (database), without capabilities;
only the one-shot volume initializer runs as root with narrowly named file
ownership capabilities. Containers have read-only root filesystems.

## First operator and readiness

Issue a fifteen-minute management token into the persistent private state volume:

```sh
docker compose run --rm --no-deps operator meerkat-recall service-token issue \
  --global-admin --subject deployment-operator --expires-in 900 \
  --scope manage:grant --scope manage:provider --scope read:space_config \
  --scope write:space_config --scope read:status --scope read:system \
  --scope write:system --output /var/lib/meerkat-recall/operator.token
docker compose exec recall meerkat-recall --api-url http://127.0.0.1:3000 \
  --token-file /var/lib/meerkat-recall/operator.token status --json
```

The command writes only the path, never the bearer. Use this token for explicit
runtime, space and current-grant configuration described in
https://docs.rkat.ai/recall/operations/service-setup. It grants no ordinary data
access. Remove the expired token file before issuing its replacement, or select
a new private filename. Mount additional provider and least-privilege worker
token files with mode 0600 owned by10001; the image accepts `OPENAI_API_KEY_FILE`,
`ANTHROPIC_API_KEY_FILE`, and `MCP_BEARER_TOKEN_FILE`. Conflicting value/file inputs
are errors. Worker grants must include the current tools' required scopes,
including `read:predicate`; a bootstrap token must never become a worker token.

`GET /v1/health` is public process liveness, used by Compose's health check.
`GET /v1/readiness` requires a bearer with `read:status` and returns 200 only when
the configured storage, recovery and required semantic workers are ready;
otherwise it returns 503. Service tokens require `X-Space-Id` and an active
current same-space `read:status` grant. Named `/v1/spaces/{space}/status`
additionally checks that space's processing approvals and local credentials;
global readiness alone does not establish named semantic readiness. A healthy Compose container is not proof of provider
readiness. Use `status --require-ready --space YOUR_SPACE` with a current scoped
status token after setup; token expiry must be rotated by your deployment.
For a Kubernetes exec readiness probe, the image includes `recall-readiness`:
set `RECALL_READINESS_TOKEN_FILE` to the mounted private bearer path and
`RECALL_READINESS_SPACE` to the authorized named space. Give the probe at least
22 seconds; the CLI preserves distinct exit codes for not-ready, unavailable
and authorization refusal. Keep `/v1/health` as the independent liveness probe.

### Connect the worker and provider

After creating your named space and explicit processing approvals using the
service-setup guide, save its authorized clearance array in
`secrets/extractor-clearances.json`. Issue a separate read-only worker token;
repeat `--space` only for the spaces this worker should serve:

```sh
docker compose run --rm --no-deps \
  -v "$PWD/secrets/extractor-clearances.json:/input/clearances.json:ro" \
  operator meerkat-recall service-token issue \
  --subject recall-extractor --space research --read-only --expires-in 86400 \
  --scope read:doc:metadata --scope read:doc:raw --scope read:entity \
  --scope read:predicate --scope read:source --scope read:assertion \
  --clearances-file /input/clearances.json \
  --output /var/lib/meerkat-recall/extractor.token
```

The bind-mounted clearance file must be readable by UID10001; it contains policy
metadata, not a key. Create and read back an active `recall-extractor` grant with
those same read scopes in **each** selected space. No worker scope authorizes
mutation, provider approval, or grant administration.

Provision the provider key through your secret custodian into
`secrets/provider-key` (mode 0600). Copy it into the private state volume without
printing it; this one-shot operator command only manages the selected file:

```sh
docker compose run --rm --no-deps --user 0:0 \
  --cap-add CHOWN --cap-add DAC_OVERRIDE --entrypoint /bin/sh \
  -v "$PWD/secrets/provider-key:/input/provider-key:ro" operator -c \
  'test ! -e /var/lib/meerkat-recall/provider-key && install -o 10001 -g 10001 -m 600 /input/provider-key /var/lib/meerkat-recall/provider-key'
cp semantic.override.example.yaml compose.override.yaml
docker compose config --quiet
docker compose up -d recall
```

The example override selects OpenAI because the generated extractor model is
`gpt-6.1-sol`. Set the exact matching account, immutable profiles and approved
operation bindings in your space, with `credential_ref: OPENAI_API_KEY`. If
using Anthropic, change the override to `ANTHROPIC_API_KEY_FILE`, the loaded TOML
extractor model, and the space's provider bindings together. Existing loaded
configurations and approved profiles are not migrated by this default change.
Embedding approval is separate: provision your pinned local FastEmbed assets or its own explicitly
approved remote embedding account. The override supplies the worker token via
`MCP_BEARER_TOKEN_FILE` and selects `research` for discovery; edit that named space
to one the worker actually has authority to read. Discovery does not grant access
to other spaces. Keep token rotation and current grant generations under your
operator's control.

For scoped readiness, issue a distinct `--read-only --scope read:status --space
research` token with that space's explicit clearances into
`/var/lib/meerkat-recall/status.token`, and create its matching active current
`read:status` grant. Then:

```sh
docker compose exec \
  -e RECALL_READINESS_TOKEN_FILE=/var/lib/meerkat-recall/status.token \
  -e RECALL_READINESS_SPACE=research recall recall-readiness
```

The checked-in override is an example until these files and grants exist. The
base installation starts authenticated but remains semantically not ready until
its required configuration is supplied; it never silently approves a provider.
The loaded TOML is authoritative for runtime/database/model settings; changing
similarly named environment variables does not override that file. Manage later
configuration through the authorized runtime API (which updates this config),
or stop the service and edit its private TOML deliberately.

## Persistence and updates

Recall's single state mount is `/var/lib/meerkat-recall`: runtime configuration in
`config/runtime.toml`, durable recovery journal in `recovery`, snapshots in
`backups`, models in `model-cache`, and temporary files in `tmp`. Logs go to
stdout/stderr; use the Docker log driver with rotation. The paired database's
store/WAL lives in the separate `database_data` volume. This separation preserves
the recovery journal independently of database replacement. Do not run two
Recall processes against the same state volume. Stop Recall before maintenance,
then the database; use a clean application snapshot before replacing images.
`docker compose down` preserves named volumes; **`down -v` deletes data and keys**.
The template names its project `meerkat-recall`. For a second isolated deployment,
use one distinct `docker compose -p NAME` consistently for every command; never
share its named volumes or attach another writer to an existing installation.

For upgrades, retain the old digest references and backup, change only `.env`
to the new verified image digests, pull, and recreate in the same deployment
directory. The initializer reuses identical keys; it refuses accidental key
replacement. Key rotation is an explicit maintenance operation: stop writers,
back up the original keys, replace both host and appropriate named-volume copies
with matching UID/mode, and reissue service tokens before restarting.

Recall-authored code is MIT OR Apache-2.0. The paired upstream SurrealDB server,
core and SDK retain their **Business Source License 1.1** terms. Their original
licenses, pinned upstream identity, Recall source provenance and reference-fence
capability stay with the image. Review those terms for your use; these recipes
do not relicense the server. A vanilla SurrealDB image is not interchangeable.
