# bet-recorder

Python CLI and worker for capturing betting state and serving normalized snapshots to Sabi.

It owns run bundles, live watcher flows, and the worker protocol used by `operator-console`.

## Develop

```bash
python -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest -q
```
