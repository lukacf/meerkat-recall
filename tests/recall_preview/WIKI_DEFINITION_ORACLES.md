The independent definition tests exercise the actual `WikiGraphPlanner` against
both supported storage backends. They are source-only until the coordinator's
central gate executes the exact joined checkpoint.

The product owner supplies this minimal child hook in
`crates/pipeline/src/maintenance_commit_tests.rs`:

```rust
#[path = "../../../tests/recall_preview/wiki_definition_oracles.rs"]
mod independent_wiki_definition_oracles;
```

The child reuses the existing backend/schema/space setup and base-field helpers.
Its own deterministic adapter inspects the original planning prompt, returns two
definitions within one exact security partition, and changes real storage during
the final organization assignment call, after the separate taxonomy call. It
requires exactly those two calls for this fixture. It does not inject a receipt, source completion, refreshed
epoch, or prepared canonical plan.

- Changing a disconnected original contributor after inventory capture must
  refuse the whole plan. No definition, view, audit, event, or original decision
  receipt may appear; the intentional contributor change must remain visible.
- A positive original plan must produce both definitions and a real canonical
  receipt. Each definition's private input proof must contain both original
  intrinsic entities. Source-less intrinsic authority remains legitimate.
- Deleting an original definition during the next organization call must refuse
  the entire replacement. The deleted row remains deleted, its original semantic
  payload stays intact, the sibling remains byte-for-byte equivalent, the
  original receipt stays intact, and no new receipt/audit/event appears.

Article generation deliberately returns a controlled error. These tests qualify
the definition boundary only. Existing actual article and unchanged-cache
positives remain required. No scripted planning result qualifies model judgment,
grounding, managed-grant authority, worker authority, or installed acceptance.

Complementary transaction tests reviewed at composed wiki source
`cf4e752be1637ff1bc353475cef5ba489f65f625` are the owner's
`wiki_definition_bundle_rolls_back_every_definition_audit_event_and_receipt` and
storage `canonical_definition_tests::actual_bundle`. The latter injects failures
after the first effect, before outbox publication, and before receipt publication;
it checks both definitions, outbox and decision receipt absent, then commits the
same original request successfully. D does not relabel these unexecuted source
tests as independent runtime observations or copy them to inflate coverage.

The separate managed article phase still requires original worker attempt and
current grant propagation into its committing transaction. Definition atomicity
does not satisfy that authority gate. Cross-partition progress and failures must
remain explicit; these fixtures do not broaden atomicity beyond one partition.
