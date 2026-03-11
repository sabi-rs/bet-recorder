from pathlib import Path
import sys
import json

from typer.testing import CliRunner

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.cli import app
from bet_recorder.exchange_worker import WorkerConfig, load_exchange_snapshot_for_config


def test_exchange_worker_session_handles_multiple_ndjson_requests(
  tmp_path: Path,
) -> None:
  runner = CliRunner()
  positions_payload_path = tmp_path / "smarkets-open-positions.json"
  positions_payload_path.write_text(
    json.dumps(
      {
        "page": "open_positions",
        "body_text": (
          "Available balance £120.45 Exposure £41.63 Unrealized P/L -£0.49 "
          "Open Bets Back Arsenal Full-time result 2.12 £5.00 Open "
          "Back Both Teams To Score Bet Builder 1.74 £3.50 Open "
          "Lazio vs Sassuolo "
          "Sell 1 - 1 Correct score 7.2 £2.55 £15.81 £18.36 £2.46 -£0.09 (3.53%) Order filled Trade out "
          "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£0.31 (3.13%) Order filled Trade out"
        ),
        "inputs": {},
        "visible_actions": ["Trade out"],
      },
    ),
  )

  load_dashboard_request = (
    fixture_text("load_dashboard_request_template.json")
    .replace("__POSITIONS_PAYLOAD_PATH__", str(positions_payload_path))
    .replace("\"__RUN_DIR__\"", "null")
    .replace("\"__ACCOUNT_PAYLOAD_PATH__\"", "null")
    .replace("\"__OPEN_BETS_PAYLOAD_PATH__\"", "null")
    .replace("\"__AGENT_BROWSER_SESSION__\"", "null")
  )
  load_dashboard_request = json.dumps(json.loads(load_dashboard_request))
  result = runner.invoke(
    app,
    ["exchange-worker-session"],
    input="\n".join(
      [
        load_dashboard_request,
        json.dumps(json.loads(fixture_text("refresh_request.json"))),
        json.dumps(json.loads(fixture_text("select_venue_request.json"))),
        "",
      ],
    ),
  )

  assert result.exit_code == 0
  responses = [json.loads(line) for line in result.output.splitlines()]

  assert len(responses) == 3
  assert responses[0]["snapshot"]["selected_venue"] == "smarkets"
  assert responses[0]["snapshot"]["account_stats"]["currency"] == "GBP"
  assert responses[0]["snapshot"]["open_positions"][0]["contract"] == "1 - 1"
  assert responses[0]["snapshot"]["other_open_bets"][0]["label"] == "Arsenal"
  assert responses[1]["snapshot"]["status_line"] == responses[0]["snapshot"]["status_line"]
  assert responses[1]["snapshot"]["watch"]["watch_count"] == 2
  assert responses[2]["snapshot"]["watch"]["watch_count"] == 2


def test_exchange_worker_session_loads_latest_positions_snapshot_from_run_dir(
  tmp_path: Path,
) -> None:
  runner = CliRunner()
  run_dir = tmp_path / "smarkets-run"
  run_dir.mkdir()
  (run_dir / "events.jsonl").write_text(
    "\n".join(
      [
        json.dumps(
          {
            "captured_at": "2026-03-11T11:00:00Z",
            "source": "smarkets_exchange",
            "kind": "positions_snapshot",
            "page": "open_positions",
            "url": "https://smarkets.com/open-positions",
            "document_title": "Open positions",
            "body_text": (
              "Available balance £120.45 Exposure £41.63 Unrealized P/L -£0.49 "
              "Open Bets Back Arsenal Full-time result 2.12 £5.00 Open "
              "Lazio vs Sassuolo "
              "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£0.31 "
              "(3.13%) Order filled Trade out"
            ),
            "interactive_snapshot": [],
            "links": [],
            "inputs": {},
            "visible_actions": ["Trade out"],
            "resource_hosts": ["smarkets.com"],
            "local_storage_keys": [],
            "screenshot_path": None,
            "notes": [],
          },
        ),
        json.dumps(
          {
            "captured_at": "2026-03-11T11:05:00Z",
            "source": "smarkets_exchange",
            "kind": "positions_snapshot",
            "page": "open_positions",
            "url": "https://smarkets.com/open-positions",
            "document_title": "Open positions",
            "body_text": (
              "Available balance £150.00 Exposure £23.29 Unrealized P/L £2.10 "
              "Open Bets Back Arsenal Full-time result 2.12 £5.00 Open "
              "Back Both Teams To Score Bet Builder 1.74 £3.50 Open "
              "Lazio vs Sassuolo "
              "Sell 1 - 1 Correct score 7.2 £2.55 £15.81 £18.36 £2.46 -£0.09 "
              "(3.53%) Order filled Trade out "
              "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£0.31 "
              "(3.13%) Order filled Trade out"
            ),
            "interactive_snapshot": [],
            "links": [],
            "inputs": {},
            "visible_actions": ["Trade out"],
            "resource_hosts": ["smarkets.com"],
            "local_storage_keys": [],
            "screenshot_path": None,
            "notes": [],
          },
        ),
      ],
    )
    + "\n",
  )

  result = runner.invoke(
    app,
    ["exchange-worker-session"],
    input=json.dumps(
      {
        "LoadDashboard": {
          "config": {
            "positions_payload_path": None,
            "run_dir": str(run_dir),
            "account_payload_path": None,
            "open_bets_payload_path": None,
            "agent_browser_session": None,
            "commission_rate": 0.0,
            "target_profit": 1.0,
            "stop_loss": 1.0,
          },
        },
      },
    )
    + "\n",
  )

  assert result.exit_code == 0
  response = json.loads(result.output)
  assert response["snapshot"]["account_stats"]["available_balance"] == 150.0
  assert response["snapshot"]["open_positions"][0]["contract"] == "1 - 1"
  assert response["snapshot"]["other_open_bets"][1]["label"] == "Both Teams To Score"
  assert response["snapshot"]["watch"]["watch_count"] == 2


def test_load_exchange_snapshot_captures_live_open_positions_before_reading_run_dir(
  tmp_path: Path,
  monkeypatch,
) -> None:
  run_dir = tmp_path / "smarkets-run"
  run_dir.mkdir()
  events_path = run_dir / "events.jsonl"
  events_path.write_text(
    json.dumps(
      {
        "captured_at": "2026-03-11T11:00:00Z",
        "source": "smarkets_exchange",
        "kind": "positions_snapshot",
        "page": "open_positions",
        "url": "https://smarkets.com/open-positions",
        "document_title": "Open positions",
        "body_text": (
          "Available balance £120.45 Exposure £41.63 Unrealized P/L -£0.49 "
          "Open Bets Back Arsenal Full-time result 2.12 £5.00 Open "
          "Lazio vs Sassuolo "
          "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£0.31 "
          "(3.13%) Order filled Trade out"
        ),
        "interactive_snapshot": [],
        "links": [],
        "inputs": {},
        "visible_actions": ["Trade out"],
        "resource_hosts": ["smarkets.com"],
        "local_storage_keys": [],
        "screenshot_path": None,
        "notes": [],
      },
    )
    + "\n",
  )

  captured = {}

  def fake_capture(config: WorkerConfig) -> None:
    captured["session"] = config.agent_browser_session
    events_path.write_text(
      events_path.read_text()
      + json.dumps(
        {
          "captured_at": "2026-03-11T11:05:00Z",
          "source": "smarkets_exchange",
          "kind": "positions_snapshot",
          "page": "open_positions",
          "url": "https://smarkets.com/open-positions",
          "document_title": "Open positions",
          "body_text": (
            "Available balance £150.00 Exposure £23.29 Unrealized P/L £2.10 "
            "Open Bets Back Arsenal Full-time result 2.12 £5.00 Open "
            "Back Both Teams To Score Bet Builder 1.74 £3.50 Open "
            "Lazio vs Sassuolo "
            "Sell 1 - 1 Correct score 7.2 £2.55 £15.81 £18.36 £2.46 -£0.09 "
            "(3.53%) Order filled Trade out "
            "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£0.31 "
            "(3.13%) Order filled Trade out"
          ),
          "interactive_snapshot": [],
          "links": [],
          "inputs": {},
          "visible_actions": ["Trade out"],
          "resource_hosts": ["smarkets.com"],
          "local_storage_keys": [],
          "screenshot_path": "screenshots/open_positions-20260311T110500Z.png",
          "notes": ["exchange-worker-refresh"],
        },
      )
      + "\n",
    )

  monkeypatch.setattr(
    "bet_recorder.exchange_worker.capture_current_smarkets_open_positions",
    fake_capture,
  )

  snapshot = load_exchange_snapshot_for_config(
    WorkerConfig(
      positions_payload_path=None,
      run_dir=run_dir,
      account_payload_path=None,
      open_bets_payload_path=None,
      commission_rate=0.0,
      target_profit=1.0,
      stop_loss=1.0,
      agent_browser_session="helium-copy",
    ),
  )

  assert captured["session"] == "helium-copy"
  assert snapshot["account_stats"]["available_balance"] == 150.0
  assert snapshot["open_positions"][0]["contract"] == "1 - 1"
  assert snapshot["watch"]["watch_count"] == 2


def fixture_text(name: str) -> str:
  return (Path(__file__).resolve().parent / "fixtures" / "worker" / name).read_text()
