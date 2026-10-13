# Installed seeded-vector negative

This implements step 7 of `tests/recall_preview_semantics/REAL_PROVIDER_RUN.md`,
within the original provider/relevance gate. It is not a real-provider,
FastEmbed inference, groundedness, hybrid, or whole-preview acceptance result.
Source controls and an authored driver are not installed execution evidence.

Each allocation selects **one seed, one persistent backend, one configuration**:

| Configuration | Household dimensions | Business dimensions |
| --- | ---: | ---: |
| `local384` | 384 | 384 |
| `primary` | 1536 | 3072 |

The original seeds are `17`, `219`, `20261006`. Each requires a distinct new
private run directory and store; both retained backends remain separate cases.
The offline Python matrix (three seeds × 8/384/1536) and Rust actual-index
384/1536 tests are unchanged and retain their separate evidence classes.
This helper never writes the accepted store or database/vector rows directly.

The seed generator exactly follows the existing Rust
`semantic-index-negative-v1` SHA256 counter recipe, including initial and final
f32 rounding. Hash input is domain bytes followed by little-endian u64 seed,
stream, coordinate. Documents use their original global corpus index; queries
use 10000 plus their original global query index. Text selects only an exact
declared ordinal; it never influences vector generation. All 28 original texts
are distinct within their spaces and equal the frozen paragraph/chunk input
spans. No trimming, anchors, fallback text, or arbitrary vector map is accepted.

The driver installs the exact allocated archive, issues real CLI-signed service
and space-bootstrap tokens, initializes a new ElephantDB snapshot or native
SurrealDB RocksDB store, and creates the original two spaces with their frozen
policies/guidance. Only document/query embedding operations are approved, through
test-only OpenAI-protocol connections to the owned loopback listener. There are
no external routes, real provider credentials, generation calls or model assets.
An unauthenticated request and a foreign-space token must be denied.

All 28 revisions are submitted in original order with real source CAS, including
the superseded visit revision. Retrieval-mode ingestion traverses the actual
governed embedding worker, physical index and source finalizer. Original
operation/head completion, immutable rendered revisions, node spans and
embedding partition identities must match before queries begin. A public final
operation and immutable revision are recorded; a private finalizer receipt is
not invented when it is not exposed.

The actual root node's serialized role is `document`; `root_span` remains the
independent rendered map's coordinate key. The source inventory binds the Rust
`NodeRole` Serialize declaration and the real node producer as raw data. Controls
exercise the actual wire spelling and reject `root`, missing/duplicate roles
and truncated spans without altering the original rendered map.

All 24 original queries use the authenticated HTTP semantic-only route with
cutoff 3 and the full current source-revision constraints. Actual wire responses
and order are retained. Partial responses, missing inventory, invalid source
evidence, missing physical provider requests or repeated original queries cannot
be counted as successful negatives. The unchanged oracle's combined retrieval
condition must fail: all individual retrieval cases passing **and** macro MRR at
least the original corpus threshold would make this negative fail. The driver
does not strengthen this to requiring both kinds of numerical failure. The
fake-vector evidence class alone never causes a passing numerical run to count
as a negative.

`ControlledSpy` has two default-preserving subclass hooks for recipe validation
and response building. Its original constant-vector mode and schema are
unchanged. `SeededSpy` supplies only a strictly frozen embedding recipe; parser,
space/model/credential matching, physical dispatch bounds, durable original
intent/capture and shutdown are the existing implementation. The actual embedding
client sends no `X-Space-Id`: the seeded subclass binds space only through each
separately configured unique synthetic credential hash and exact model/path. It
records the original absent header separately and refuses a conflicting supplied
header. It never infers scope from input text or call timing. Raw provider
requests/responses, typed public publications, original queries, configuration
before/after and store path/device/inode before/after are preserved. The new
store is absent before setup; post-run store identity is evidence, not proof of
physical erasure. The store remains isolated for independent inspection.

`installed_fake_vectors.py plan` emits an inert exact source/input inventory.
The inert `installed_fake_vectors_manifest.example.json` shows the run manifest;
its format is `recall-installed-fake-vectors-run/v1` and its exact
fields are enumerated in `validate_manifest`. It binds source/archive/binary
identities, fresh run directory and nonce, backend, seed/configuration, original
fixture timestamp, explicit expiry and **original outer allocation clock**,
both fresh named spaces, zero external sends/paid calls, and an independently
reviewed exact plan file/hash. That review is
`recall-installed-fake-vectors-review/v1`, reviewer
`qualification (independent D lane)`, `approved: true`, with the exact `plan`
object. The root-approved manifest SHA256 is passed separately.

The reviewed outer owner creates the fresh network namespace and starts the
original clock before installation. Within that namespace the concrete command
is `python3 -I -B scripts/qualification/installed_fake_vectors.py run
--manifest /absolute/run.json --manifest-sha256 <approved-sha256>
--execute-reviewed --parent-network-namespace <original-parent-netns>`.
This is a command contract, not an execution grant. The same retained resource
ledger/floor and idle scheduling are the outer owner's responsibility; this
driver neither acquires nor resets those allowances. Whole allocation is at
most 1800 seconds, including a 90-second cleanup reserve. Source publication
has a 1200-second ceiling within that original work window. All cleanup and
terminal sealing consume the same clock. The outer owner must retain actual
exit status, observation, full capture tree and positive process/namespace
cleanup before D can consider the negative observation complete.

Listener cleanup latches a close request before startup can finish, always
attempts bounded loop stop and thread join even when the close future fails,
and propagates the original failure. A late startup cannot enter `run_forever`
after cleanup. Thread/task absence is required; a clean ownership shutdown does
not erase an earlier timeout or failed call.

The closed-world tests are in `installed_fake_vectors_test.py`. They cover
original seed/text/ordinal/configuration contracts, vector formula/batch order,
undeclared requests, numerical false-positive prevention, unchanged old spy
defaults/refusals, physical input omission/repetition, incomplete provider
captures, store reuse and incomplete shutdown. They do not replace the installed
run. No test or installed run was executed at this source checkpoint.
