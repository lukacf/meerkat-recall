# MCP work-item attempt contract

The retained `claim_work_item` and `complete_work_item` tools bind completion to the original claim. They keep their pipeline/full profiles and explicit selected-space requirement. Both require `read:work_item` and `write:work_item` and current authorization for the actual stored row. Read-only runtime and readiness gates also apply.

A claim still accepts:

```json
{"id":"work_item:550e8400-e29b-41d4-a716-446655440000","worker_id":"worker-1","lease_duration_secs":300}
```

The lease duration must be positive and fit a valid expiration timestamp; an invalid duration returns `invalid_request`.

Its successful existing `ToolResponse` envelope adds the exact opaque token:

```json
{"ok":true,"data":{"id":"work_item:550e8400-e29b-41d4-a716-446655440000","status":"running","claimed":true,"worker_id":"worker-1","lease_token":"opaque-original-attempt-token","lease_expires_at":"2026-10-07T12:05:00Z"}}
```

The token is the committed attempt identity. The human worker label is not that identity. A later claim, including one by the same worker, receives a different token. Clients must retain the token with their original task and send it without normalization:

```json
{"id":"work_item:550e8400-e29b-41d4-a716-446655440000","lease_token":"opaque-original-attempt-token","success":true}
```

`success` defaults to true. For a failed attempt use `success:false` and optionally `error`. Success returns `{"ok":true,"data":{"id":"work_item:…","status":"done","completed":true}}`; failure completion returns terminal `failed`. These are work-queue outcomes. Source publication still requires its canonical operation and publication checks.

| Condition | Public MCP code | Retry unchanged |
| --- | --- | --- |
| Missing, null, empty or whitespace-only token, or invalid claim duration | `invalid_request` | No |
| Nonempty token differs from the original live attempt, including a worker-label substitute | `conflict_generation` | No |
| Selected-space item is absent, or the original attempt was completed, replaced or lost its lease | `conflict_generation` | No |
| Stored authority changes between authorization and the atomic mutation | `conflict_generation` | No |
| Actual item exceeds current record clearance or required scopes | `forbidden_space` | No |
| Source erasure temporarily reserves the space | `not_ready` | Yes |

At the HTTP MCP transport boundary these refusals use `isError:true` and the standard `{error_code,message,retryable}` body. The claim/completion handlers return these refusals as typed MCP errors. A nonretryable conflict means reconcile the old attempt; it does not authorize replacing its token with a successor's token. Completion after an already terminal attempt conflicts instead of reporting a second success.

Storage compares the authorized observed record in the same transaction as claim or completion, together with the original live lease for completion. The handler cannot adopt a token by loading the current row. Existing internal queue APIs remain available to their scheduler owners; this public contract creates no second queue authority or synthetic WorkRun.

Source checkpoints: Recovery `5d98aea2d1c27c7337d00e699928a13f60880e3f` implements the guarded handlers/repositories; `5087427` completes typed preflight refusal and checked lease-time validation. Both author backend tests; Surface `d545a1e` makes capability discovery require both scopes. Independent qualification must use real claim, original-token requeue and same-worker reclaim rather than inserting a fabricated token. Source review and authored tests are not runtime qualification. No provider or model invocation is required for this contract's local queue tests.
