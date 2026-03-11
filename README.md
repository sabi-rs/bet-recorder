# bet-recorder

Recorder-first CLI for capturing browser-visible betting data into append-only run bundles.

Current implemented scope:
- run-bundle creation
- page snapshot writing
- sanitized transport-event writing
- browser payload models and adapter helpers
- direct `agent-browser` client for current-page extraction
- direct CDP transport capture against a live Chromium/Chrome debug port
- screenshot persistence helper
- fixture-backed live capture runner
- source-specific page collectors for:
  - `rebelbetting_vb`
  - `rebelbetting_rb`
  - `fairodds_terminal`
  - `profitmaximiser_members`
- Smarkets watch extraction and grouped trade-out thresholds
- exchange-worker stdio commands for `operator-console`
- long-running Smarkets watcher process with persisted `watcher-state.json`

## Development

```bash
python -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest -q
```

Repo-local launcher:

```bash
./bin/bet-recorder --help
```

## Exchange Worker Transport

`operator-console` can use `bet-recorder` as a worker-backed Smarkets snapshot source.

One-shot request/response:

```bash
printf '%s' '{"LoadDashboard":{"config":{"positions_payload_path":"/tmp/smarkets-open-positions.json","run_dir":null,"account_payload_path":null,"open_bets_payload_path":null,"commission_rate":0.0,"target_profit":1.0,"stop_loss":1.0}}}' | \
  ./bin/bet-recorder exchange-worker-stdio
```

Persistent session:

```bash
./bin/bet-recorder exchange-worker-session
```

The session command reads newline-delimited JSON requests from stdin. The first
`LoadDashboard` request must include `config`. Later `Refresh` and `SelectVenue`
requests reuse that session config until the worker exits.

The current config shape is:
- `positions_payload_path` optional
- `run_dir` optional
- `account_payload_path` optional
- `open_bets_payload_path` optional
- `agent_browser_session` optional

At least one of `positions_payload_path` or `run_dir` must be provided. When `run_dir` is used,
the worker reads the latest `positions_snapshot` event from `events.jsonl`.

If `run_dir` and `agent_browser_session` are both set, the worker captures the current Smarkets
`open_positions` page from that `agent-browser` session into the run bundle before it reads the
latest snapshot.

The worker snapshot can now include:
- grouped watch thresholds
- raw open positions
- account stats
- other open bets

The preferred path is a richer `open_positions` capture whose body text already contains account
summary and visible open bets. The optional account/open-bets payload paths are still supported as
fallback inputs when those sections are captured separately.

Real-data console flow:

```bash
bet-recorder init-run \
  --source smarkets_exchange \
  --root-dir /tmp/bet-recorder-demo \
  --started-at 2026-03-11T11:05:00Z \
  --collector-version dev \
  --browser-profile-used helium-copy

bet-recorder record-page \
  --source smarkets_exchange \
  --run-dir /tmp/bet-recorder-demo/captures/smarkets_exchange/2026/2026-03-11/run-20260311T110500Z \
  --payload-path /tmp/smarkets-open-positions.json
```

Then start the console with:

```bash
cd /home/thomas/projects/sabi/console/operator-console
cargo run -- \
  --bet-recorder-run-dir /tmp/bet-recorder-demo/captures/smarkets_exchange/2026/2026-03-11/run-20260311T110500Z \
  --bet-recorder-session helium-copy
```

## Smarkets Watcher

Run a foreground watcher process that captures the current Smarkets `open_positions` page from an
`agent-browser` session, recomputes grouped watch rows, and writes the latest state to
`watcher-state.json` in the run bundle:

```bash
bet-recorder watch-smarkets-session \
  --run-dir /tmp/bet-recorder-demo/captures/smarkets_exchange/2026/2026-03-11/run-20260311T110500Z \
  --session helium-copy \
  --interval-seconds 5 \
  --commission-rate 0 \
  --target-profit 1 \
  --stop-loss 1
```

The watcher currently classifies each grouped lay as:
- `take_profit_ready`
- `stop_loss_ready`
- `hold`
- `monitor_only`

The decision is based on grouped live `current_pnl_amount` versus the configured
`target_profit` / `stop_loss`, while the state also preserves the precomputed profit-take and
stop-loss back odds for operator reference.

## Example Workflow

Initialize a run bundle:

```bash
bet-recorder init-run \
  --source rebelbetting_vb \
  --root-dir /tmp/bet-recorder-demo \
  --started-at 2026-03-09T10:15:00Z \
  --collector-version dev \
  --browser-profile-used helium-copy \
  --transport
```

Record one page snapshot from a JSON payload:

```bash
bet-recorder record-page \
  --source rebelbetting_vb \
  --run-dir /tmp/bet-recorder-demo/captures/rebelbetting_vb/2026/2026-03-09/run-20260309T101500Z \
  --payload-path /tmp/vb-dashboard.json
```

Record one transport event from a JSON payload:

```bash
bet-recorder record-transport \
  --source rebelbetting_vb \
  --run-dir /tmp/bet-recorder-demo/captures/rebelbetting_vb/2026/2026-03-09/run-20260309T101500Z \
  --payload-path /tmp/vb-transport-event.json
```

Finalize the run metadata:

```bash
bet-recorder finalize-run \
  --source rebelbetting_vb \
  --run-dir /tmp/bet-recorder-demo/captures/rebelbetting_vb/2026/2026-03-09/run-20260309T101500Z \
  --ended-at 2026-03-09T10:20:00Z
```

## Live Fixture Workflow

The live runner currently reuses the same payload contract as `record-page`, but
it exposes explicit commands for browser-derived payloads:

```bash
bet-recorder capture-live-page \
  --source rebelbetting_vb \
  --run-dir /tmp/bet-recorder-demo/captures/rebelbetting_vb/2026/2026-03-09/run-20260309T101500Z \
  --payload-path /tmp/live-page.json
```

```bash
bet-recorder capture-live-transport \
  --source rebelbetting_vb \
  --run-dir /tmp/bet-recorder-demo/captures/rebelbetting_vb/2026/2026-03-09/run-20260309T101500Z \
  --payload-path /tmp/live-transport.json
```

The current live runner is fixture-backed rather than browser-driven. Its purpose is
to preserve a stable ingestion contract while the browser automation layer is added.

## agent-browser Workflow

Capture the current page from an `agent-browser` session directly into the run bundle:

```bash
agent-browser --session helium-copy open https://example.com

bet-recorder capture-agent-browser-page \
  --source rebelbetting_vb \
  --run-dir /tmp/bet-recorder-demo/captures/rebelbetting_vb/2026/2026-03-09/run-20260309T101500Z \
  --page dashboard \
  --captured-at 2026-03-09T10:16:00Z \
  --session helium-copy \
  --note smoke-test
```

This command reads the current page from the named `agent-browser` session, captures:
- title and URL
- interactive refs from `snapshot -i`
- body text, links, inputs, visible actions, and resource hosts via `eval`
- local storage keys via `storage local`
- a bundle-owned screenshot under `screenshots/`

The resulting page event is appended to `events.jsonl` using the same recorder contract
as the manual and fixture-backed workflows.

## CDP Transport Workflow

Capture live transport events from an already running Chromium-based browser that exposes
 a DevTools port, such as Helium on `127.0.0.1:9222`:

```bash
bet-recorder capture-cdp-transport \
  --source rebelbetting_vb \
  --run-dir /tmp/bet-recorder-demo/captures/rebelbetting_vb/2026/2026-03-09/run-20260309T101500Z \
  --duration-ms 5000 \
  --debug-base-url http://127.0.0.1:9222 \
  --url-contains vb.rebelbetting.com
```

If the selected page is idle, the recorder may append zero `Network.*` events during the
capture window. To deliberately trigger fresh transport activity on the selected target,
use `--reload`:

```bash
bet-recorder capture-cdp-transport \
  --source rebelbetting_vb \
  --run-dir /tmp/bet-recorder-demo/captures/rebelbetting_vb/2026/2026-03-09/run-20260309T101500Z \
  --duration-ms 5000 \
  --debug-base-url http://127.0.0.1:9222 \
  --url-contains vb.rebelbetting.com \
  --reload
```

This command records sanitized `Network.*` CDP events into `transport.jsonl` and annotates
each event with the matched target id, title, and URL.
