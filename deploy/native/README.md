# Independent native service

Templates do not install or start services. Select a verified native archive,
retain its complete tree and use an absolute install directory. Linux uses the
included systemd unit; Darwin ARM64 uses the launchd template. Substitute XML
escaped absolute paths for `__INSTALL_DIR__`, `__STATE_DIR__` and
`__RECOVERY_DIR__` before loading launchd. Never put the recovery directory inside
the replaced data directory or restore it from an older data snapshot.

Create private state, data, recovery, secrets, model-assets, model-cache and log
directories owned by the service account. Bootstrap a signing key with `service-token bootstrap`, then
issue explicit administrative and consumer grants/tokens as described in the
[operations guide](https://github.com/lukacf/meerkat-recall-releases/blob/main/docs/operations.md) (also `docs/operations.md` at the native archive root). Configure the same backend/namespace/database for `migrate`
and `serve`; run explicit initialization before the service. `serve` verifies
schema and refuses uninitialized persistent storage. Provider approvals and
per-space configuration are required separately for model operations.

The service takes SIGTERM or SIGINT, withdraws readiness, drains managed workers
and HTTP requests, then persists the authoritative embedded snapshot path. Its
60-second deadline is shorter than the manager's 75-second kill deadline. A
failed drain/flush exits unsuccessfully; it is not a clean shutdown receipt.
A stable store ownership file prevents overlapping local writers even during
replacement. Keep that file; deleting it defeats inode-based exclusion.

Probe `status --json --require-ready --space NAME` using a private bearer file.
The liveness endpoint alone does not qualify ingestion. Install/start/upgrade
and production selection are operator actions, separate from archive creation.
Record exact artifact hashes and a restore rehearsal before switching services.

For the isolated model-backed wiki demo, copy
`demo-wiki-processing.example.json`, replace its account/model/version values,
and set the environment variable named by `credential_ref`. Pass that file with
`demo-wiki --processing-config PATH`. The explicit planner/writer/verifier
bindings use the ordinary per-space provider authority. The file contains no
credentials and authorizes only the isolated demo's named `default` space;
it does not configure this persistent service.


Native candidates include the `rocksdb,fastembed` features and retain the default
Meerkat processing feature. The model cache is outside the immutable installation
and belongs to the service user. Set `FASTEMBED_CACHE_DIR` as shown; leave
`HF_HOME` unset or point it to the same directory. The governed FastEmbed
connection uses an explicit `file:///.../model-assets` endpoint and a `sha256:...` model version for the manifest installed with
`provision-fastembed` helper bundled beside the executable. Its verified ONNX/tokenizer bytes are loaded
without downloads or mutable-name fallback. These files are separate from the
executable archive; missing or altered assets prevent provider use. The cache
environment variable applies to retained ungoverned local tools only. Record
asset file hashes with the bundled
`artifact.py model-cache-receipt` command when qualifying a concrete artifact.
Provider grants, exact space bindings and credentials remain separate setup.

A verified prebuilt Apple Silicon archive avoids compiling on the consumer Mac.
If building locally, the release recipe defaults to four jobs and a 32 GiB free
disk preflight; 35–45 GiB free is a possible build allocation, not a measured peak
guarantee. The binary's recorded `otool -L` inspection must contain only system
library paths. Do not transfer a build that depends on the builder's Homebrew
installation or run multiple builds against the same target cache.
