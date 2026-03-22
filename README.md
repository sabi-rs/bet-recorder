# bet-recorder

Python CLI and worker for capturing betting state and serving normalized snapshots to Sabi.

It owns run bundles, live watcher flows, and the worker protocol used by `operator-console`.

## Owns

- Recorder CLI and worker entry points
- Watcher state, run bundle artifacts, and normalized live snapshot generation
- The newline-delimited JSON protocol consumed by `operator-console`

## Integrates With

- `../console/operator-console` as the main consumer of recorder state and control messages
- `../workers/exchange-browser-worker` when live browser automation is required

Changes to snapshot shape, watcher-state behavior, or worker message contracts should be treated as integration changes, not local refactors.

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
