# Native original-ancestry observation regressions

The independent test module
`crates/storage/src/work_producer_observation_tests.rs` exercises the retained
private `WorkProducerRepository::lineage_dependencies` entry and actual
`enqueue_successor` against native memory storage. It is based on integration
source `48a26c391d4e429520d2dcde86d374e0144dea30`. Recovery owns the bounded native
observation implementation and the sibling module declaration in
`work_producer_enqueue.rs`; this test-only change must be composed with that
product change before central validation.

Seven controls establish real independent Work admission, actual leases and
original-fence WorkRun receipts, then create descendants through the retained
atomic producer. Expected observations compare every complete typed row and
private origin in original traversal order. No positive fixture invents an
origin, a successful source publication or an execution attempt.

The controls cover:

- Actual three- and four-Work chains and their complete ordered Work/Run/origin
  observations, including successful admission after observation.
- Missing projected parent Work/Run, substitution of another admitted ancestor,
  wrong table, plain string and wrong native record-ID kind, mismatched current
  Work identity and foreign private-control space. Each negative is bracketed
  by positive observations after restoring the same original value.
- A false terminal with both parent hints absent, a projected cycle, an extra
  edge after an actual Direct terminal, and an explicit Unknown origin. None
  may become independent authority.
- Corrupt original payload, a native-UUID private-key alias in place of the
  actual string-key control, and physically missing private evidence.
- Foreign, deleted and physically missing original ancestor Work/Run rows.
  One deleted Run is restored from the exact original typed receipt solely to
  distinguish the missing-Run refusal from the subsequent missing-Work refusal.
- Actual completion and archival of both ancestors, exact cold observations,
  actual descendant admission through final transaction guards, and refusal
  when a required original archived Run disappears.
- Actual independent ReprocessJob admission and an inherited job created with
  the existing sealed-parent factory and original parent claim. Both Job rows,
  the original parent Run and both private origins retain their exact order;
  removing inherited-parent hints cannot relabel that job independent.

Every negative tests both observation refusal and actual enqueue refusal, checks
absence at the producer's derived physical child ID and its private origin, and
checks that the leaf's original Work and Run remain unchanged. Controlled raw
mutations are invalid test inputs applied only after genuine admission. Test
comparison uses JSON serialization of domain values, never a database proxy or
custom-deserializer intermediary.

The five previously passing native guard-batch controls remain byte-unchanged.
They retain exact original before-image, NONE/native-type, physical hot/cold,
duplicate-descriptor and transaction nonce-rollback coverage. The original
256-Work/257-Source ancestry scenario and its 240-second deadline also remain
unchanged and must be included in central validation. This small independent
module does not replace that full bound/performance oracle or establish a
performance result.

The original boundary fixture now retains the genuine 257th Work's original
Run and attempts one further successor admission from that same chain. It must
refuse with the original producer-depth conflict and leave neither the derived
physical child ID nor its private origin. Both backends inspect actual private
storage; native count decoding requires `Some(0)`. The fixture retains every
original 256-success/257-accounting refusal assertion, test name, source graph
and the same 240-second whole-case deadline. No second chain is constructed.

Only direct Rust formatting, source review and Git diff checks were performed
locally. No Cargo command, Rust test body, provider call or runtime process was
executed in this worktree for this checkpoint. The parent owns the coherent
compiled validation and the resulting evidence.
