# Qualified container delivery

Release images belong to the canonical `lukacf/meerkat-recall` repository:
`ghcr.io/lukacf/meerkat-recall` and `ghcr.io/lukacf/recall-surreal`.
Build them once from the coherent release source using the existing scheduled
Recall and paired-server build recipes. These are Linux amd64 images. The
paired-server source, reference-fence capability and upstream BUSL-1.1 notices
remain independent from Recall's MIT OR Apache-2.0 license.

Keep the original image receipts, source provenance and actual runtime
qualification private. The release owner selects exact immutable image IDs
only after actual host-port, authenticated Compose and paired capability
qualification, including the generated Compose init/database/migrate/service
sequence (a standalone `docker run` does not substitute for installation). Local `RepoDigests` are not proof that an image is available in
a registry. Do not rebuild in the publication job or substitute vanilla SurrealDB.

Create a private selection JSON with this shape (all identities are actual
qualified values, not placeholders at execution):

```json
{
  "approved": true,
  "reviewer": "release-owner",
  "host_port_smoke_passed": true,
  "compose_installation_passed": true,
  "paired_capability_passed": true,
  "source_commit": "FULL_RELEASE_COMMIT",
  "version": "0.9.1",
  "images": {
    "recall": {"image_id": "sha256:CONFIG_DIGEST", "binary_sha256": "BINARY_SHA256", "build_receipt": "/private/release/ACTUAL_RECEIPT.json", "build_receipt_sha256": "RECEIPT_SHA256"},
    "paired": {"image_id": "sha256:CONFIG_DIGEST", "binary_sha256": "BINARY_SHA256", "build_receipt": "/private/release/ACTUAL_RECEIPT.json", "build_receipt_sha256": "RECEIPT_SHA256"}
  }
}
```

Scheduled export runs no container or compiler:

```sh
python3 scripts/release/container_delivery.py export \
  --selection /private/release/selected-images.json --output /private/release/container-delivery
```

It refuses absent qualification, wrong canonical source/platform/license labels,
missing paired capability identity, existing output, insufficient actual free
space, or archives exceeding the GitHub release asset limit. It verifies exact
archive hashes and Docker configuration digests without extracting or executing
image members. Preserve failed export output; select a fresh directory after
diagnosis. No cache pruning or disk-accounting reset is performed.

Review the three exported files, then attach them to the selected draft release:
`container-delivery.json`, `meerkat-recall.docker.tar`, and
`recall-surreal.docker.tar`. Dispatch **Promote qualified Recall containers** at
the exact qualified source commit, with the draft release tag and the reviewed
manifest SHA256. The workflow uses the canonical repository's `GITHUB_TOKEN`
with only `contents:read` and `packages:write`. It loads and publishes the exact
images after checking the actual Git tag resolves to the same qualified source.
Both registry version tags are checked before publication; a different existing
config digest or registry/authentication failure refuses promotion. Same-byte
retry reuses the published digest. Canonical workflow concurrency serializes
publishers; GHCR has no compare-and-swap against unrelated external writers.
The job never recompiles or starts either server. Keep draft assets private
until the release owner completes publication review.

New GHCR packages can initially be private. The package owner must make both
packages public and link them to the canonical repository; repository visibility
alone does not establish package visibility. The workflow preserves actual
registry digest records even if its anonymous pull check fails. Resolve package
visibility without rebuilding or changing the qualified image. Before announcing
ready-to-pull images, verify both exact registry digests anonymously from a clean
Docker configuration and run the authenticated Compose smoke on the selected
host. Use those digests in `prepare-compose`; never publish invented digests.

GitHub's authentication and package visibility rules are documented at
https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry.

## Actual host-port check

After scheduled Compose startup and explicit runtime/space/worker provisioning,
select the running Recall container's full ID and exact published loopback port:

```sh
python3 scripts/release/host_port_smoke.py --container-id "$OWNED_CONTAINER_ID" \
  --port 3000 --token-file /private/release/status.token --space qualification \
  --require-ready --output /private/release/host-port-result.json
```

The helper requires Docker's actual `127.0.0.1` mapping before and after three
bounded HTTP calls. It verifies public health, unauthenticated readiness refusal,
and authenticated readiness. Missing Docker port bindings fail; it cannot use a
relay or report the previous release's relay transport as host-port acceptance.
This is a deployment smoke, separate from the existing semantic/provider oracle.

## Native macOS inference worklist

The macOS owner binds a fresh0.9.1 archive and original resource/workload limits
after the coherent source is frozen. Reuse model cache only after verifying its
exact model files and receipt. Verify/install the archive into a new owned
directory; preserve its actual build source and the machine's macOS/arm64 identity.
Run native help/version plus one original local model inference/document and two
semantic queries using the existing native qualification oracle. Capture actual
ONNX loading, vector dimensions, complete publication, raw citation bytes and
authenticated refusal; do not infer inference success from Mach-O/static analysis.
Retain timeouts, cancellation and cleanup receipts, all attempted physical work,
and any original failure before a prospective repair. Do not replay accepted0.9
work or claim older-macOS support from a newer host. This checklist is preparation,
not an execution or acceptance record.
