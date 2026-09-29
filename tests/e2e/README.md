# E2E verification

Run the deterministic integration gate with:

```bash
./scripts/repo-cargo test-e2e
```

This includes ignored database scenarios and the source-first extraction fixture.
The extraction fixture invokes the production work-kind dispatcher with a fixed
extractor response, then verifies source lifecycle, committed assertions, outbox
changes, and searchable entities. It does not test model quality.

Live model extraction, calibration, and local media benchmarks are excluded by
test name (`live_llm`, `real_llm`, `real_example`, and `calibrate_`). Alice and
Wizard of Oz use the `live_llm` suffix. Run them explicitly with credentials
configured for `EXTRACTION_MODEL` (default `gpt-5.2`):

```bash
./scripts/repo-cargo test-e2e-live
```

These scenarios use the same source-first dispatcher and artifact commit boundary.
A missing provider credential or an extraction failure fails the live run. A
passing deterministic gate is not evidence that the live scenarios passed.
