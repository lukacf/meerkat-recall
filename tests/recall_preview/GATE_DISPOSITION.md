# Preview qualification disposition

Source review: 2026-10-07 00:29 UTC. **No whole gate below is passed.**
Full workspace/all-target/all-feature compilation passed with zero errors and
warnings at `4a27e208b793e9ee6021f8ff08741bab358f15e2`. Central runtime1 at
that exact source executed 2719 tests: 2440 passed, 265 failed, 14 aborted;
101 skipped tests are separate. Overall runtime failed. Subsequent owner fixes
and later D test source require a new compile/runtime result; they are not
covered by the earlier pass. Central integration owns all Rust execution.
Central standalone1 at `d3e8f8852981b19366e18d696e248b83976aecba`
executed117 tests:78 passed,37 failed,2 aborted,0 skipped; overall exit100.
Its 21 failures assigned to D include missing original render-fence fixture
setup and native named-restore schema replay. D's fixture repair is committed;
F owns the product restore repair. Neither repair has a subsequent runtime result.
Five new D contact-lineage methods remain unexecuted and require F's module hook.

`matrix.py` provides exact targets, expectations, peer authorship and source pins.
Here, **ready** means a concrete offline harness exists and can be scheduled after
its coherent source join. **Partial** identifies missing harness or fixture code.
Artifact, provider and consumer evidence are separate from both. Scripted spies,
fixed interpretations, random vectors, skipped tests and missing prerequisites
cannot establish real semantic or deployment acceptance.

| Gate | Available source/harness | Required next evidence or missing code |
| --- | --- | --- |
| D01 exact lineage | Ready: renderer/planner/finalizer and classified evidence | Central both-backend scenarios |
| D02 negative grounding | Ready: invalid hashes/quotes/spans/lineage and fixed unsupported output | Central both-backend scenarios |
| D03 independent support | Ready: fixed verifier, tampered staging, long quote and public corrections | Central scenarios; model quality remains separate |
| D04 atomic publication | Ready: actual transaction faults and retry reconciliation | Central both-backend scenarios |
| D05 revision/CAS | Ready: exact input, concurrent CAS, retries and A→B→A | Central repository/public HTTP scenarios |
| D06 attempt ownership | Ready: storage fences and new public MCP token oracle | Join B/C token, typed-refusal and date-bound successor before execution |
| D07 quiet recovery | Ready: all claim selectors, two quiet spaces, finalizer discovery | Central scenarios; complete installed worker restart separate |
| D08 success-only cursor | Ready: production delivery, durable effect, failure/retry and SIGKILL | Central scenarios/process tests; downstream model decisions separate |
| D09 retract/support | Ready: published graph, stale work and independent A/B support | Central both-backend scenarios |
| D10 physical erase/TTL | Ready source: graph/work/populated hot-cold archives/trusted shared media/actual source and mixed views/native references/live files/backup expiry | Central both-backend execution; installed physical evidence later |
| D11 restore barrier | Ready: old/pre-source backups, original journal and reserved replay | Central both backends; two-process paired server and installed volumes separate |
| D12 authority | Ready: signed HTTP/MCP, next-request grants, ABAC and inventory | Join contact/attempt successors; installed/persistent-session variants separate |
| D13 provider dispatch | Ready: actual configured transports/adapters and revocation spies | Central deterministic tests; real account/credential custody separate |
| D14 profile isolation | Ready: rotation/partition/cache tests and owner coverage activation | Central repository/index suites, independently review activation outcome |
| D15 errors/bounds | Partial: in-process inputs, typed errors, modes and collector framing | Installed transport driver plus complete output/time observations missing |
| D16 2 MB source | Ready: actual HTTP ASCII/Unicode/encoded-overhead acceptance | Central acceptance tests; full extraction/index/resource gate remains |
| D17 retained surfaces | Partial: source/lifecycle adapters and wiki/TM/identity spies | Contact040/reprocess/restore joins and complete writer-catalog review |
| D18 retrieval semantics | Frozen corpus/evaluator/collector and golden-run preparation | Gateway successor with reviewed full-context reservation or counted contract; exact artifact and paid authorization |
| D19 no fake vectors | Ready negative control: actual-index random vectors plus frozen evaluator | Central control, then actual provider relevance; dimensions/variance insufficient |
| D20 native artifact | Ready Linux source: installed driver, 19 cases, readback and owned cleanup reviewed at S7f1029f | Exact joined hash-custody record and central installed execution; Darwin ARM64 execution still separate |
| D21 container/PV | F eight-case image/PV driver reviewed; persisted configuration readback gap reported | Complete configuration baseline checks, exact joined review hashes and actual image/PV execution |
| D22 save integrity | Ready source: corruption/WAL process cases and15 independently reviewed configuration write/activation fault definitions | L installed capped-filesystem driver pending; actual OS-full/interrupted-save evidence separate |
| D23 initialization | Schema failure paths and paired-server preparation exist | Installed initialization failure/retained-transfer refusal observations missing |
| D24 truthful usage | Ready: receipt/partial/terminal/decoder/restart owner suites | Central accounting tests; actual charges and priced currency separate |
| D25 typed retry | Ready: original claim, finite exhaustion, blockage and stale owners | Typed temporary-erasure successor joined; central tests pending |
| D26 temporal truth | Fixed decision/inverse/current-support cases; frozen temporal corpus | Central mechanics, then actual provider temporal/corroboration/policy capture |
| D27 wiki usefulness | Ready mechanics: editorial compiler/catalog/publication races | Real writer/verifier usefulness and citation review need authorized execution |
| D28 consumer workload | Ready prequalification expectations:50 total sources,200calls/50callers, all-hit evidence and measured overlap; S bounded collector | G exclusive-cohort authority and S actual preparation; proposed2100 physical attempts/$3 awaits final guard review. Larger/~140-search and other workload phases remain separate; no load executed |
| D29 OB3 fallback | Expected outage/degradation contract | OB3 owns consumer code and acceptance evidence |
| D30 optional HomeCore | Retained standalone source and consumer expectations | HomeCore owns startup/transition/rollback; installed standalone Recall subset separate |
| D31 field observation | Failure criteria only | Agree consecutive-day count and separately authorize cutover/observation |
| D32 planning read sets | Ready: identity phantom and owner record/predicate guards | Central both-backend tests; full writer-catalog review still needed |
| D33 exact coverage | Ready: pending/current revision, real index and completion scenarios | Central scenarios, then installed public observations |
| D34 patched PDF | Ready:13 frozen synthetic PDFs and16 actual parser/intake cases | Central parser target; resource-bound tests and exact installed artifacts remain separate |
| D35 decision inverse | Ready: actual A/B source decisions and independent nested baseline | Central both backends; pending contact lifecycle join adds further cases |
| D36 inventory | Ready: current ABAC, pagination, actual tombstones and cursor binding | Central both backends; installed timing/scan-budget evidence separate |
| D37 acknowledged crash | Ready: actual owned SIGKILL after accept/claim/whole publication | Persistent ElephantDB; external Surreal and installed artifacts remain separate |
| D38 erase/restore crash | Ready: eight SIGKILL erasure points, live payload and WAL isolation | Persistent ElephantDB; paired server and installed volume recovery separate |
| D39 wiki fence | Ready: mutation after writer start and malformed/downgraded proof | Central both backends; editorial quality remains D27 |

## Assigned missing harness

At root's explicit 23:24 request, D assigned S new
`scripts/qualification/installed_linux_offline.py` and
`tests/recall_preview_semantics/installed_linux_*` paths. First freeze the actual
CLI/auth/config/API sequence and expected outcomes. Then implement an independent
driver for an explicitly supplied verified Linux native bundle: owned temporary
files/processes, bounded loopback requests, real synthetic authentication, exact
artifact/source identity and private raw evidence. Unmet prerequisites must refuse.

Priority is installed auth, HTTP/MCP, two named spaces, metadata-only acceptance
and durable restart. Source and stub tests are authorized; artifact/service
execution stays centrally scheduled. Models, downloads, production, Darwin,
container/PV, full Graph semantics, PDF and consumer workload remain separate evidence.
D retains expectations and verdicts; L/C supply actual packaging/launch contracts.

The primary provider proposal's two concurrent requests are not the D28 load
gate. Parent clarified that OB3 reported about 140 workers over 51 minutes, not
140 simultaneous requests; actual concurrency must be measured under the agreed
workload. OB3 requires physical deletion plus bounded backup retention. Recall's
journal/restore replay is an additional Recall requirement, not an OB3 gate.
