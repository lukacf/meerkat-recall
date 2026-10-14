# Original-journal historical erasure commitments

The original erasure intent and reference membership become durable before
logical purge. Under the same maintenance reservation, the native history
inspection follows logical purge. Exact causal version proofs are then synced
to the original journal before physical generation replacement. The native
copier must compare the complete inspected scope fingerprint before applying
the proof. The journal does not infer causality from content equality.

The existing checked `reference-membership-index.json` payload gains one
optional final field, `history_intents`, a sorted set of canonical intent UUIDs.
An empty set is omitted, preserving the original empty-index bytes and checksum.
The reference index's format version and checksum domain remain unchanged.
This required original anchor detects a missing acknowledged history file,
including loss of the entire history directory. It is not a separate index
which could disappear together with its files.

Each `history-commitments/<intentUUID>.json` contains `{payload,checksum}`. The
payload fields, in serialization order, are `version` (1), `journal_identity`,
`key_fingerprint`, `intent_id`, `intent_digest`, and `scopes`. The last field is a
sorted map from an opaque scope digest to `{history_fingerprints,erased_versions}`;
both values are sorted sets of lowercase 64-hex commitments. No raw native key,
row, source payload, namespace, database name, or reference secret is stored.
Current-control retention exceptions are not persisted or reused.

The scope digest hashes the compact JSON tuple `[namespace,database]`; the intent
digest hashes the original typed intent's compact JSON serialization. The file
checksum hashes the payload's compact JSON serialization. Each uses SHA-256
with the prefix `meerkat-recall/history-journal/v1` followed by a NUL byte. Scope
and commitment sets are monotonic across retries. Observed fingerprints record
the inspected scopes; they do not authorize applying a stale snapshot.

Writers use the original reference-membership OS lock and journal mutex. They
publish and sync the complete proof file, then publish and sync acknowledgment
in the existing index. A crash between those publications leaves a complete,
strictly validated unacknowledged file that a same-intent retry merges and
acknowledges before returning authority. Missing acknowledged files, damaged
checksums, wrong identities, foreign entries, broad file permissions, symlinks,
and hardlinked proof files block startup. Unacknowledged files never suffice to
authorize a physical rewrite. Reads and writes refuse files over 128 MiB, more
than 1,000,000 total commitments/fingerprints, or more than 1,024 native scopes.
The qualification collector retains its separate existing evidence-size bound.

Eight source controls in `erasure_history_journal_tests.rs` cover independent
writer union, restart, file and whole-directory loss, file-before-index crash
recovery, explicit empty acknowledgment, scope isolation, original-intent and
checksum refusal, payload-free bytes, old empty-index compatibility, and Unix
alias refusal. They are ordinary storage unit tests. The component checkpoint
does not claim executed tests or complete historical causality; the separate
projection, native inspection/copy, and restore integration must be composed and
validated before making that claim.
