# Original producer ancestry oracles

`producer_ancestry_oracles.rs` is an independent child of the actual pipeline
`reprocess::budget` module. M owns the hook and the signed fixture helpers. The
four test entrypoints cover both supported backends; there is no provider call.

The boundary fixture uses an actual signed Watch registration and queued,
claimed root, then 255 actual private successor admissions with original runs.
The 256-Work chain must pass the full physical-dispatch preflight. A shared
accounting walk must additionally verify 257 separately admitted Direct Source
operations, then the entire Work chain, without halving the supported Work
bound. Revisited successfully verified branches must not consume another node.
The actual 258th Source and 257th Work must fail for the execution bound. The
full dispatcher must also refuse the oversized Work chain. Actual root-grant
revocation must invalidate a new dispatch of the previously successful chain.

The shared-walk Source fixture checks the two independent counting domains with
real persisted operation descriptors. It does not claim a single recursively
alternating Watch/Source graph, completed extraction, or a live model result.
Existing M/B tests separately cover actual Watch-to-Source execution ancestry,
inherited budgets, and independently admitted reprocess roots. Keep those gates.

The smaller fixture removes the original attempt fence from the real parent
run. A shared walk must fail, then succeed after restoring exactly the original
evidence. A second case explicitly enters the original root as an active
traversal ancestor: traversing its actual private child must detect a cycle;
after unsuccessful unwind the original graph must succeed. This active-path
control is not a fabricated persisted cyclic producer record.

The long fixture has a 240-second whole-case bound, below the original helper's
five-minute claim lease. The short fixture is bounded at 60 seconds. No spawned
tasks, polling waits, special worker threads, increased stack, or private origin
writes are used. The long fixture performs many real ancestry reads during
successor admission; central scheduling owns its eventual execution.

Required owner hook in `crates/pipeline/src/reprocess/budget.rs`:

```rust
#[cfg(test)]
#[path = "../../../../tests/recall_preview/producer_ancestry_oracles.rs"]
mod independent_producer_ancestry;
```

This checkpoint is source-only. Direct rustfmt and diff checks do not establish
compilation, backend acceptance, execution timing, or cleanup. It requires M's
separate 256-Work/257-Source accounting implementation and B's original standing
authority source graph, and is not an addition to the frozen Runtime12 graph.
