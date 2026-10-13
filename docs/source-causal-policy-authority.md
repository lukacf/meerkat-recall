# Retained causal policy authority

`SourcePublicationRepository::has_causal_source_support` checks both physical source-effect support and retained source-decision effects with nonempty causal source IDs. A legacy source-backed decision can have only the latter. Clearing a record's visible provenance must not turn that decision into independently supplied global policy.

This read is deliberately conservative. It does not certify current publication, factual evidence, or retrieval eligibility. Maintenance captures its original canonical epoch before reading authority and commits against the original row, source fences, and epoch. Withdrawal removes invalidated decision controls through the existing lifecycle transaction.

Both-backend canonical inverse regression source asserts that a real legacy derived commit has no publication-support row but does have causal decision authority; unrelated records and spaces remain false. Tests are authored and have not been run in the integrity worktree. Central qualification owns execution.
