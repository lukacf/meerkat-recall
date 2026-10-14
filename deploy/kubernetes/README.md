# Independent container service

This is a single-node deployment recipe, not a deployment or acceptance record.
Build an exact committed candidate with `scripts/release/build-container`, qualify
it, and select its registry digest. Render `RECALL_IMAGE_BY_DIGEST` in all four
container entries before applying the manifest. Publication, cluster credentials
and deployment are separate operator actions.

The CSI driver must support `ReadWriteOncePod`. Set a suitable StorageClass and
volume sizes for the corpus. The three claims are deliberately distinct:

- `data`: embedded RocksDB and the stable sibling writer-lock inode.
- `recovery`: independently durable erasure intents, restore marker and owned
  backup retention registry. Never roll this claim back with the data claim.
- `backups`: named immutable snapshot bundles. Keep it mounted during retention
  cleanup; external copies need the same or a shorter independent expiry policy.

`replicas: 1`, ordered startup and `OnDelete` updates avoid intentional overlap;
RWOP prevents attaching the data claim to another pod, and the process-held
storage lock refuses another local writer even before the engine opens. Never
force-delete a pod whose node may still be writing, delete the lock inode, or
mount copies of a live volume as independent writers. External SurrealDB and
ElephantDB remain supported; this recipe selects one concrete persistent shape.

Create the `meerkat-recall-auth` Secret with a generated `JWT_SECRET`, using the
same secret for the server and token-provisioning containers. Do not commit its
value or include it in qualification evidence. `service-token bootstrap` can
create a private local key; supply those bytes via the platform secret manager.
Keep the secret outside data backups: preserve it separately under the secret
manager's recovery policy, or deliberately reissue all service tokens after a
signing-key change. The recipe does not print signing keys or issued bearers.

Initialization is the explicit `schema` init container (`migrate`). The server
only verifies schema. Configure approved providers and named spaces through the
public administration contract. On a fresh store, use an administrative token
and a pod port-forward while normal readiness is false to create the `operations`
space and a live `recall-readiness` grant containing `read:status`. Change
`RECALL_PROBE_SPACE` explicitly if choosing another administrative space.
Ordinary consumers need their own explicit named-space grants; token signing
alone does not create a live grant.

Liveness is unauthenticated `/v1/health`, without provider/storage probes.
Readiness executes `status --require-ready --space operations` with a private
bearer file. The init container issues a one-hour token; a small sidecar replaces
it atomically every thirty minutes under the same subject. Both tokens remain
valid during overlap, subject to the current grant. Rotation failure causes
readiness to fail when the remaining token expires; it never enables skip-auth.
The process signals drain on SIGTERM, fails readiness first, and has 60 seconds
to drain/flush inside the pod's 75-second termination allowance.

The supplied service has no public ingress. If remote callers need access,
configure authenticated TLS ingress and network policy in the cluster's own
infrastructure. Recall is independent: consumers may use their own authorized
fallback while it is absent or unready.

For backups, invoke the installed CLI against the service with a global snapshot
operator token. `snapshot export --output /var/lib/meerkat-recall/backups/NAME`
quiesces through the runtime and returns a versioned digest manifest. Retention
is seven days by default and at most thirty days; the service processes its
owned retention registry at startup and every sixty seconds. Archive copies,
cloud disk snapshots and processor retention are outside that registry and
require independently configured finite deletion policies. Restoring requires
the separately retained recovery claim; readiness waits for erasure replay.
