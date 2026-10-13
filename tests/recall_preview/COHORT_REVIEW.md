# Independent exclusive-cohort review

D source review covers G cohort implementation `2a0eef967e3c7f3900f3a3d4d9a54242970096f1`,
typed-envelope pins `9349de2c469a9306c1730e9f89e8464e723b002d` and runtime
skills path successor `bd7373e9d5023c70fe3799776ea3a85a42d75931`.
Current G source is `83060dcc6f05e602ae221061255cb71251138429`; its later
Rust stack repair does not change these gateway modules.

The original four single-use cohorts, physical and per-space attempt caps,
nonrefundable embedding reservation, typed preparation pins, durable original
send IDs, admission closure and exact private capture inventory are present in
source. The 20 GiB capture reserve is conservative for the frozen 2100 sends.
The close operation can truthfully return unmet while original dispatches remain
active; it cannot authorize the next cohort. Per-public-request attribution is
explicitly unmet. Closed cohort replay must use the same original observation.

`cohort_contract.py` authors twelve independent offline cases using the actual G
SQLite ledger, capture writer, transport and stdlib HTTP parser. It freezes numeric
limits separately, checks original send intent from a second read-only SQLite
connection, checks repeated identical bodies retain different charged IDs, closes
during/before an original send, reopens a closed ledger, rejects wrong profile /
credential generation / backend, rejects oversized batches and forged bounds,
exercises fifty reserved measurement slots, preserves the original deadline,
corrupts a captured request, supplies a genuinely short HTTP body and refuses an
empty successful cohort. The policy case copies D's exact allocation and typed
files into a private temporary directory and verifies changed typed bytes and
revoked approval refuse. Its synthetic constructor inputs are not live approval.
No socket, subprocess, provider key or installed artifact is permitted.

The coherent source suite passed all12 offline cases in0.577s against G
`83060dcc6f05e602ae221061255cb71251138429`, with no skips and all eight module
hashes unchanged across execution. The [exact receipt](receipts/cohort-offline-83060dcc.json)
records the test source and module hashes. This is a passing offline accounting
observation, not product acceptance. This suite does not make2100 network requests, prove real
provider pricing, exercise the full isolation launcher or qualify measurement
latency. The original larger consumer workload remains separate.

Open integration review: the measurement collector must bind begin phase/backend/
allocation and original cohort/baseline to the close, require durable closed /
zero-unsettled state and sealed hashes, and place absolute deadlines around its
synchronous begin/close calls. Preparation already uses bounded wrappers; current
G control bridge only has per-socket timeout. D requested coherent S/G successors,
with wrong-cohort and stalled-close negatives. Host namespace availability is
reported blocked by C's actual probe, not by this offline suite; no fallback or
spending authorization is implied.
