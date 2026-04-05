# bet-recorder

Legacy Python fallback capture and worker boundary for Sabi.

It still owns run bundles, live watcher flows, and the newline-delimited worker protocol used by `operator-console`, mainly for markets without API adaptors and for historical data extraction while a better solution is being developed.

## Owns

- Recorder CLI and worker entry points
- Watcher state, run bundle artifacts, and normalized live snapshot generation
- The newline-delimited JSON protocol consumed by `operator-console`

## Integrates With

- `../console/operator-console` as the main consumer of recorder state and control messages
- `../sabisabi` when recorder-derived state needs to line up with persisted backend APIs
- `../workers/exchange-browser-worker` when live browser automation is required

`bet-recorder` is still an active runtime, but it is a fallback path in the larger Sabi stack rather than the preferred architecture.

Expect it to be higher latency and more brittle than adaptor- or API-backed ingestion paths.

Changes to snapshot shape, watcher-state behavior, or worker message contracts should be treated as integration changes, not local refactors.

## Worker Notes

- `SelectVenue` is intentionally non-blocking. It switches the selected venue and reuses cached state when available instead of forcing an immediate live browser capture.
- `RefreshLive` is the operation that performs a fresh live capture for non-`smarkets` venues.

## Develop

```bash
python -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest -q
```

Prefer the smallest regression test that proves the change:

```bash
.venv/bin/pytest tests/test_watcher_state.py::test_build_watcher_state_marks_profit_and_stop_loss_readiness -q
```
