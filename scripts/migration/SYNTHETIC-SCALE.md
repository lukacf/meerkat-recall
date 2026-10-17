# Synthetic initial-path scale driver

`synthetic_scale.py` is for the existing qualification owner after functional
controls pass. It never builds, starts workers, calls a model provider, reads
household files, or runs automatically. Use an actual CLI executable, not a Rust
test harness. Supply its exact digest and the qualification source manifest.

Each run creates a new private directory. Its clock starts before deterministic
snapshot generation, the matching synthetic ledger backup and input hashing.
It then invokes the real planner with fresh ledger-derived identities and the
real `data-migrate --preservation-plan` command. No generated dataset can be
reused through the `run` entry point. Original outputs and failures are retained;
there is no automatic rerun or target deletion.

The `1k` and `10k` profiles have 1,000 and 10,000 documents respectively, four
nodes per document, original revisions, source-less entities, hot and archived
work/runs, staged artifacts, audit/cost history, events, and finite 3072-dimensional
legacy vectors across four partitions. The `household-core-shape` profile uses
7,518 documents, 187,614 nodes, 179,684 vectors and the retained major operational
table counts. It explicitly omits parts of the real graph/media/source and
revision/deletion distribution; it is not a full household replica. Every
`input.json` lists those omissions.

Example for the sole execution owner, with fresh resource admission and actual
binary/source hashes filled in:

```sh
python3 scripts/migration/synthetic_scale.py run \
  --profile 1k --output /qualified/new-scale-1k \
  --workspace /qualified/exact-source \
  --binary /qualified/meerkat-recall --binary-sha256 "$BINARY_SHA256" \
  --source-manifest /qualified/source-manifest.json \
  --source-manifest-sha256 "$SOURCE_MANIFEST_SHA256" \
  --planner-sha256 "$PLANNER_SHA256" \
  --max-snapshot-bytes 67108864 \
  --max-run-bytes 2147483648 --disk-reserve-bytes 268435456 \
  --max-seconds 180
```

Start with `1k`; choose a separate fresh directory and explicit resource bounds
for each larger profile. The owner must admit disk and memory for the selected
fixture. Snapshot writes have an exact byte ceiling. Total run disk is sampled
every second with an explicit reserve; it is not a filesystem quota and can
overshoot between samples. The driver ends only its own subprocess group on a
limit or interruption. It does not constrain unrelated workloads. No hard RSS
limit is implied. A household vector fixture may require many GiB of original,
spooled and native storage: no default household execution budget is supplied.

Per-stage logs and GNU-time records retain elapsed time, peak RSS in KiB, exit
status and exact commands. The transfer report adds per-table and internal phase
timings. Assertions check exact source table counts, actual SQLite inventory
counts, source digest, copied counts, held work counts, closed ledger/bindings
hashes and the completed transfer state. Native full-row reconciliation remains
the real command's responsibility; count equality alone does not replace it.

`SYNTHETIC_INITIAL_PATH_PASS` proves only the measured synthetic initial path.
It excludes actual original-store export, transport, final delta, provider
embedding, activation/certification and consumer switch. The required fresh
household path through certification must still complete within 3,600 seconds,
with every phase inside that clock and independently retained receipts. Do not
extrapolate that claim from either small profile or from a partial shape.
