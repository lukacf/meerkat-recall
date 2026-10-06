# Work queue capacity and retention

Accumulated terminal work can overload the database even when the running binary has not changed. Unindexed scheduler reads decoded accumulated terminal jobs, and embedded SurrealDB queries competed with API handlers on the runtime where the client connected. Caller timeouts did not stop engine work. Low process CPU and a responding lightweight health endpoint do not establish application readiness.

The capacity controls combine indexed scheduler queries and exact aggregate counts, durable archival of terminal work, and engine execution deadlines. Runtime isolation, background-only conveyor status, and broader maintenance/cancellation changes remain follow-up work.

## Queue queries

Migration 028 adds indexes for global queue status, scoped status/kind, idempotency keys, source work, execution attempts, and dependency edges. It is additive and does not impose uniqueness on existing idempotency keys.

An index declaration alone is insufficient on SurrealDB 2.6.5. A disjunction in the runnable predicate caused `EXPLAIN` to choose a table scan even with an index hint. The scheduling predicates use an equivalent `array::any` expression so the planner selects the queue index while retaining delayed-work behavior. Tests verify 17 actual query plans, including expired-lease recovery and both hot/cold failed-work recovery selectors, and NONE/NULL handling. Archival preloads indexed open-attempt references once per pass; it does not scan completed attempts for each candidate. The candidate predicate also handles the planner evaluating record functions without a current row.

Runnable counts use a database aggregate instead of the selector's bounded candidate window. Counts are exact above 10,000. The existing running-by-kind aggregate is retained and covered by the same scale regression.

## Historical work

Terminal jobs and completed execution attempts move to durable cold tables in bounded passes. Their original public IDs, security, payloads, failures, provenance, and usage metrics remain available. Historical reads and idempotency lookup include cold records. Retrying a terminal job restores its original ID to the active queue atomically.

Active jobs, open execution attempts, and durable wiki authority are protected. Archival does not erase failed dependencies or let a source claim graph readiness after a failed current run. Document pipeline status reads matching document/revision/source history directly, including archived work; it no longer depends on the first 1,000 unrelated jobs in the space. An extraction that completed with zero assertions still reports completed.

The work-history worker runs independently of legacy cleanup, immediately at startup and then every 60 seconds by default. Each pass archives at most 1,000 jobs and 1,000 completed attempts. `PIPELINE_WORK_HISTORY_POLL_INTERVAL` and `PIPELINE_WORK_HISTORY_BATCH_SIZE` configure this cadence and batch size (effective ranges 1-86,400 seconds and 1-1,000 records). The existing `PIPELINE_RETENTION_DAYS` age cutoff defaults to 30 days. Throughput still needs observation under production load.

Dependency promotion resolves only the incoming predecessor IDs, including deterministic cold record IDs, within its update transaction. A missing, failed, or cancelled predecessor remains a blocker. Constructing a cold record ID from the whole native record link preserves its UUID type; reconstructing it from a string would miss the archived record.

Archive tables grow as retained history; the hot scheduler tables no longer need to hold every completed execution forever. This is archival rather than destruction of diagnostic evidence.

## Engine deadlines

Embedded SurrealDB receives finite query and transaction limits at connection time, defaulting to 30 seconds. Configure positive integer seconds with `SURREALDB_QUERY_TIMEOUT_SECS` and `SURREALDB_TRANSACTION_TIMEOUT_SECS`, or the corresponding runtime storage configuration fields. Restart the process to apply a deadline change to an open persistent embedded database. Live reconfiguration cannot reopen the same RocksDB store while its existing connection owns the store lock.

SurrealDB 2.6.5 stores its transaction option without enforcing it. Meerkat Recall therefore gives the engine the shorter of the two configured limits as the query-batch deadline. A long explicit transaction is cancelled and rolled back under that deadline. This also means a multi-statement query can end at the shorter transaction setting.

Remote SurrealDB ignores these client-side execution options. Set a finite deadline on the database server itself. The repository's Compose service supplies `--query-timeout`, configurable with `SURREAL_QUERY_TIMEOUT` (default `30s`). A timeout on the caller alone does not prove database cancellation.

## Activation and mitigations

Deploy through your service activation procedure with a recoverable database backup. Apply the additive schema before relying on its query plans, then start the binary using finite engine deadlines. Observe retention and queue latency while the historical backlog drains. Index creation on existing data has a one-time cost; measure it on a restored snapshot before activation. If migration needs a longer deadline, raise both finite `SURREALDB_QUERY_TIMEOUT_SECS` and `SURREALDB_TRANSACTION_TIMEOUT_SECS` for that command, since the shorter governs. For a remote database, adjust the server deadline separately. Restore normal limits afterward. Migrations 028 and 029 check statement errors and must complete successfully before activation; a timeout is not a successful migration.

Depending on workload, consider these operator changes:

- Remove the stale `PIPELINE_REQUEUE_FAILED_BEFORE` and `PIPELINE_REQUEUE_FAILED_ERROR_MATCH` overrides once their intended recovery is complete.
- Reduce `PIPELINE_MAX_CONCURRENT` and `PIPELINE_MAX_EXTRACT_CONCURRENT` to fit the host's available CPU and memory.
- Increase work queue and maintenance poll intervals if repeated sweeps exceed useful throughput.
- Avoid an expensive full queue-status probe after every ingest, or bound its work and timeout deliberately.

Apply configuration to the environment actually loaded by the service, then verify the effective values after restart.

## Acceptance

The explicit scale fixture is run with:

```sh
./scripts/repo-cargo test --locked -p elephant-storage --lib \
  queue_indexes_scale_past_fifty_thousand_and_counts_are_exact -- --ignored --nocapture
```

It adds 50,000 historical jobs, measures claim/idempotency/count latency with a fixed active set, asserts bounded latency growth with an allowance for measurement noise, checks queue plans use indexes, and proves exact counts with over 10,000 active jobs. Two additional explicit scale tests cover the actual failed-work recovery, dependency promotion, and archival paths:

```sh
./scripts/repo-cargo test --locked -p elephant-storage --lib \
  dependency_promotion_stays_bounded_with_fifty_thousand_cold_items -- --ignored --nocapture
./scripts/repo-cargo test --locked -p elephant-storage --lib \
  retention_pass_stays_bounded_with_fifty_thousand_completed_attempts -- --ignored --nocapture
```

Observed local five-sample medians before/after adding 50,000 unrelated records:

| Operation | Before | After |
| --- | ---: | ---: |
| Claim candidates | 23.58 ms | 19.79 ms |
| Hot idempotency lookup | 2.00 ms | 1.52 ms |
| Runnable aggregate | 6.94 ms | 6.04 ms |
| Actual dependency promotion | 4.49 ms | 4.46 ms |
| Cold idempotency lookup | 2.88 ms | 2.77 ms |
| Actual archival pass | 8.72 ms | 10.59 ms |

These memory-engine/debug results were collected during concurrent local compilation. They are not production RocksDB p99 claims.

Ordinary tests cover engine deadlines and rollback after caller abandonment; archival and retries; stable historical reads; failed dependency and source-readiness barriers; and retained document status. Before production acceptance, repeat against a representative RocksDB snapshot with concurrent pipeline, ingest, and MCP search traffic, and verify `/v1/runtime/status` and `search_all` p99 below two seconds. That live load target requires measurement; deterministic tests alone cannot establish it.
