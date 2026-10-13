# Installed evidence predicates

`installed_evidence_validator.py` consumes original, typed, hash-bound byte
references without opening files or executing producers. The installed consumer
replays the frozen nineteen cases and retains its 1 MiB per-log limit. The shared
`validate_process_proof` entry point checks original process custody and retains
the current-runtime 4 MiB per-log limit.

The shared entry point also recognizes F's explicit snapshot inspection stream
contract. It requires exactly the original Recall executable followed by
`snapshot inspect --input <absolute canonical bundle> --json --include-records
--compact-records`. The existing executable hash and original child identity
checks still apply. No other command receives a larger output allowance.

The original pre-spawn intent may add `streams`, exactly two objects in stdout,
stderr order. Each has `path`, `kind`, `maximum_bytes`, and `reserved_bytes`.
Paths must equal the original `log_paths`. Stdout has kind `snapshot-inspection`
and both limits 33,554,433 bytes; stderr has kind `process-log` and both limits
1,048,576 bytes. The stdout ceiling includes the CLI's final newline. Unknown
roles, altered limits or reservations, changed paths, and extra fields refuse.
The original persisted intent event must match and precede the original binary
and spawn events. The installed nineteen-case consumer rejects this extension.

Only that original stdout reference can use the larger read bound and report
kind. Reusing its hash under a different kind refuses. Terminal success still
requires complete original EOF captures, successful exit and reaping under the
unchanged deadline. Overflow, late declarations, incomplete logs and failed
processes cannot produce a qualified report prefix.

F's producer reserves both complete stream ceilings against the caller's
existing shared capture allowance before spawning. This predicate verifies the
original declared reservation; it does not observe the caller's reserve callback
or grant additional space. The caller must bind its reviewed producer and
original aggregate accounting. Snapshot decoding, physical erasure, and backup
acceptance remain separate predicates.

The literal transcript controls cover the exact report and stderr ceilings,
role/reservation/path/command mutations, original event order, late declarations,
EOF/reaping refusal, same-byte kind conflicts, and unchanged ordinary limits.
They establish synthetic predicate behavior only, never actual process or
physical snapshot acceptance.

The installed plan retains its four original role pins: `driver_sha256`,
`process_capture_sha256`, `collector_sha256` and `installer_sha256`. The dynamic
review binds these through canonical source paths in `helper_sha256`, matching
the workload orchestrator's actual source reader:

| Installed plan role | Dynamic review helper path |
| --- | --- |
| driver | `scripts/qualification/installed_linux_offline.py` |
| process_capture | `scripts/qualification/installed_process_capture.py` |
| collector | `scripts/qualification/recall_preview.py` |
| installer | `scripts/release/artifact.py` |

Each original plan hash must equal the corresponding reviewed source hash.
Logical `installed_*` aliases cannot replace these paths. Artifact identities,
original nineteen-case predicates, process custody and bounds are unchanged.
`installed_helper_composition_test.py` passes the independent literal transcript
through the actual source verifier, freshly loaded predicate and original-byte
resolver for both backends and workload profiles. Its positive and negative
controls establish only synthetic composition behavior, never actual installed
or provider qualification. The original input graphs need a final reviewed
source-pin refresh before a separately authorized run.
