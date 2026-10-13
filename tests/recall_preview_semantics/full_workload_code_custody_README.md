# Full workload original executable bytes

The CLI bootstrap uses only standard-library code to open and hash the original
launch manifest and allocation. Before executing any product helper, it reads
all eighteen fixed source files once through bounded canonical regular-file handles
and matches the complete original allocation pin set. The additional source
read limits are one MiB per file and eight MiB total; the original input budget,
runtime clocks, provider prohibition and workload acceptance rules stay intact.

The workload, runtime-owner, inspection, installed-process and artifact helpers,
independent D oracles and CLI functional module are
compiled from those exact verified byte snapshots with `dont_inherit=True`.
Their module metadata uses the original file paths. Canonical module names are
temporarily replaced with these fresh modules and restored after success,
failure or cancellation. The original import search path is restored too.
No cached candidate module, `.pyc`, second source read
or ambient oracle import supplies executable code. Inventory and execution
receipts use the hashes of these executed snapshots. Inventory remains a
read-only, unapproved source discovery command.

The original first two launch-file byte strings are passed to the verified
transport decoder and counted in the same original aggregate input budget.
The CLI passes the exact loaded `evaluate_receipt_bytes` callable using A's
`Driver(..., ob3_oracle=...)` seam. A owns the independent oracle consumer and
refuses required evaluation when that callable is missing; this bootstrap adds
no consumer verdict or copied oracle predicate.

F's installed helper receives the six exact `VERIFIED_DEPENDENCIES` entries
before its source executes: `fixtures`, `matrix`, `oracle`,
`installed_d_capture`, `installed_release_artifact` and
`installed_original_process_capture`. Each contains the actual already-executed
module, canonical source path, original compiled source bytes and approved hash.
This carrier is created internally after the complete pin check; no launch JSON
can provide it. F's injected mode must refuse missing or substituted dependencies
without loading files. S also checks the returned module identities and original
hash registry. The pure `snapshot_inspection_capture` helper avoids importing a
container driver just to inspect a report. C's inspector and shared pure recipe
contract, and B's owned runtime module, are explicitly pinned in the same set.
Their exact owner source remains an integration prerequisite; these controls use
synthetic modules and do not execute actual installed operations.

The run manifest and original allocation now bind a pre-output `runtime_plan`
path/hash. External token files and prewritten startup receipts are refused.
Runtime inputs must agree with the original case-plan configuration, worker
recipe, data and revocable principals, controlled credential, allocated artifact,
endpoints and output directory before any output or startup. B still validates
its complete runtime schema and owns all actual artifact/process/token actions.
The resolved plan's `runtime_principal_templates` retains I's exact already
D-approved manage/revocable inputs. S compares each full subject/scopes object
against those literals as well as checking the original data grant and FW10
subject. Missing maps or altered management/read scopes refuse before output.

At command entry S records the integer monotonic/wall/monotonic sample before
reading any inputs. After every code pin matches, F's verified
`pin_allocation_clock(..., original_sample=...)` consumes those same three facts
without resampling. Backend and whole deadlines share that original origin;
input checking and helper loading consume the allocation too. The original
eighteen source files, 1800/3600-second ceilings and one GiB capture are retained.

The CLI starts the controlled spy, enters the actual B owned-runtime context,
then captures C's `initial` boundary before constructing A's Driver. It passes
that same owner as `service_control` and `capacity_capture`, and the same bound
inspector callback as `runtime_inspection`. No named-client replacement or token
mapping mode is added. B reserves 75 cleanup seconds within the original
deadline. Spy startup has a timer at that active end, B's entry has its own timer
at the same active end, and the inspector/Driver body uses that same end. Owner
exit sits outside the active timers, so a normally finishing case cannot have
cleanup cancelled when the active boundary passes. Both normal and timed-out
bodies unwind under the remaining original allocation. Startup, inspection, timeout
and cleanup failures retain a terminal CLI observation even without a Driver.
The next backend requires the original prior CLI receipt to retain successful
owner cleanup as well as a closed spy. These observations remain unqualified.

Controls import only the CLI bootstrap and synthetic temporary helper modules.
They cover source mutation after snapshot, full pin refusal before imports,
cached-module isolation/restoration, original annotation compiler behavior,
oracle callable identity through the actual CLI with inert objects, and bounded
regular-file refusal. Missing transitive pins and nested dependency replacement
refuse; both module identities and import paths are restored on failure. They
establish code custody mechanics, not runtime,
provider, consumer or product acceptance.
Additional inert lifecycle controls verify initial-before-Driver ordering, the
same owner/callback/oracle identities, refused or boolean inspection, cleanup on
the original active timeout, incomplete cleanup, the entry clock preceding input
reads, and pre-output runtime/case-plan mismatches. They run no actual service,
process, socket, namespace, artifact installer or provider helper.
A separate normal-completion control delays mocked cleanup across the active
boundary and verifies it completes within the unchanged terminal deadline.
