# Intake future allocation

Runtime11 reported stack overflows in the real facade contact-replacement test and four direct/queued Watch contracts. Those tests already pinned their outer futures. Source inspection found that the public facade/Watch state machines still embedded contact preparation, canonical retirement preview, source acceptance and queue submission state machines through multiple unboxed asynchronous layers.

The source repair adds heap allocation at those production boundaries, including shared intake preparation/acceptance wrappers and the contact projector's canonical replacement preview. It preserves the same execution order, errors, read sets, original attempts, receipts and cancellation behavior. It does not change worker stack limits or move the tests to larger stacks.

The existing facade replacement and direct/queued loopback Watch contracts now additionally assert that each public future occupies at most 64 KiB before polling. All semantic assertions remain. These bounds exercise the caller-facing allocation contract; they are not a substitute for executing the original failing tests or measuring nested polling stack use.

This checkpoint has only direct rustfmt and diff-check validation. No Cargo or runtime tests were executed in this lane. Central validation must rerun `text_replacement_preserves_original_contact_class_and_receipt`, `changed_text_is_staged_receipted_and_enqueued`, `changed_text_preserves_original_admitted_contact_capability`, `queued_contact_watch_unavailable_plan_is_terminal_without_check_mutation` and `queued_watch_retries_return_original_receipt_without_new_observation`. The source inspection identifies an allocation risk; runtime stack-overflow closure remains unverified.
