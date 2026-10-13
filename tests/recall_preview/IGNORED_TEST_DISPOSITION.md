# Exact runtime2 ignored-test disposition

The [101-case inventory](ignored-test-disposition.json) is reconstructed from
frozen source `d3e8f8852981b19366e18d696e248b83976aecba`, its module declarations
and the central runtime2 receipt. It records each fully qualified package/test,
source path, original byte hash, ignore annotation and actual prerequisites.
No Cargo command, compiled listing, test binary or service was run for this audit.

The count reconciles exactly: 53 `elephant-e2e` cases, 39 scenario definitions
included again under `elephant-scenarios::scenario`, seven `elephant-storage`
cases and two `elephant-pipeline` cases. The two live calibration integration
tests are outside that run's `--lib --bins` selection and are not added to 101.

| Disposition | Count | Actual dependency |
| --- | --- | --- |
| Offline memory repositories/scenarios | 58 | Native kv-mem and explicit test schemas |
| Offline API fixtures | 14 | In-process Axum oneshot, synthetic injected principals, mem:// |
| Offline publication with local spies | 6 | Actual source/worker fixture and controlled loopback embedding transport |
| Offline pure redaction | 2 | In-memory values only |
| Offline embedding storage fixtures | 2 | FakeEmbeddingProvider; not semantic acceptance |
| Offline event boundaries | 3 | Native memory time/status queries, minimal or 001–010 schema |
| Separately scheduled offline scale | 4 | 50,000+ history/ledger rows and actual indexed operations |
| Excluded live generation | 6 | Paid/model configuration and original provider evidence |
| Excluded real-media models | 6 | Approved media/tool inputs and model allocation |

Thus **89 are offline selection candidates; 12 require separate live allocation**.
This does not predict that the 89 pass. Older direct record, route/auth and source
fixtures may disagree with current publication guards. Those are source defects
or contract findings to investigate; an ignore annotation is not evidence that
an external service is required. Fake vectors, manually populated assertions and
injected principals cannot qualify semantic quality or installed signed auth.

Root authorized centrally scheduled audited offline E2E and scale selections
after the combined repair gate. D supplies exact proposed filters:

- [85 regular offline cases](ignored-offline-regular-filter.txt).
- [Four scale cases](ignored-offline-scale-filter.txt), scheduled separately and
  serially, with the existing 250 GiB free-space floor and bounded central resources.

The four scale tests are:

- `elephant-storage work_item_repository::queue_index_tests::queue_indexes_scale_past_fifty_thousand_and_counts_are_exact`
- `elephant-storage work_item_repository::queue_index_tests::dependency_promotion_stays_bounded_with_fifty_thousand_cold_items`
- `elephant-storage work_item_repository::queue_index_tests::retention_pass_stays_bounded_with_fifty_thousand_completed_attempts`
- `elephant-storage cost_ledger_daily_repository::index_tests::budget_reads_stay_indexed_past_fifty_thousand_historical_ledgers`

The three event cases are
`elephant-storage event_repository::tests::boundary_mode_time_range_query_respects_window`,
`boundary_mode_time_range_query_respects_window_on_migrated_schema` and
`boundary_mode_status_filter_query_respects_statuses`, with that same module prefix.
They require no external database. The 010-schema fixture is intentionally a
boundary fixture; it cannot stand in for complete current-schema acceptance.

The central executor must use ignored-only selection, retain the exact selected
test inventory/results and exclude the 12 live cases. Source changes after the
frozen audit require checking affected fixture/provider prerequisites. The
prepared filters authorize neither a run nor access to credentials or media.
