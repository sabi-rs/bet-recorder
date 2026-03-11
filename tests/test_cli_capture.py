from pathlib import Path
import sys
import json

from typer.testing import CliRunner

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.cli import app


def test_cli_root_succeeds_and_lists_available_commands() -> None:
  runner = CliRunner()

  result = runner.invoke(app)

  assert result.exit_code == 0
  assert "Commands" in result.output


def test_init_run_creates_bundle_and_prints_metadata_path(tmp_path: Path) -> None:
  runner = CliRunner()

  result = runner.invoke(
    app,
    [
      "init-run",
      "--source",
      "rebelbetting_vb",
      "--root-dir",
      str(tmp_path),
      "--started-at",
      "2026-03-09T10:15:00Z",
      "--collector-version",
      "test-v1",
      "--browser-profile-used",
      "helium-copy",
      "--transport",
    ],
  )

  assert result.exit_code == 0

  payload = json.loads(result.output)

  assert payload["source"] == "rebelbetting_vb"
  assert payload["transport_capture_enabled"] is True
  assert Path(payload["run_dir"]).is_dir()
  assert Path(payload["metadata_path"]).is_file()


def test_record_page_appends_event_and_finalize_run_updates_metadata(
  tmp_path: Path,
) -> None:
  runner = CliRunner()

  init_result = runner.invoke(
    app,
    [
      "init-run",
      "--source",
      "rebelbetting_vb",
      "--root-dir",
      str(tmp_path),
      "--started-at",
      "2026-03-09T10:15:00Z",
      "--collector-version",
      "test-v1",
      "--browser-profile-used",
      "helium-copy",
    ],
  )
  assert init_result.exit_code == 0
  init_payload = json.loads(init_result.output)

  payload_path = tmp_path / "vb-dashboard.json"
  payload_path.write_text(
    json.dumps(
      {
        "page": "dashboard",
        "url": "https://vb.rebelbetting.com/",
        "document_title": "Value betting by RebelBetting",
        "body_text": "25 value bets",
        "interactive_snapshot": [{"tag": "A", "text": "Filters"}],
        "links": ["https://vb.rebelbetting.com/filters"],
        "inputs": {"search": ""},
        "visible_actions": ["Filters"],
        "resource_hosts": ["vb.rebelbetting.com"],
        "local_storage_keys": ["Token"],
        "screenshot_path": "/tmp/dashboard.png",
        "notes": ["trial-mode"],
        "captured_at": "2026-03-09T10:16:00Z",
      },
    ),
  )

  record_result = runner.invoke(
    app,
    [
      "record-page",
      "--source",
      "rebelbetting_vb",
      "--run-dir",
      init_payload["run_dir"],
      "--payload-path",
      str(payload_path),
    ],
  )
  assert record_result.exit_code == 0

  finalize_result = runner.invoke(
    app,
    [
      "finalize-run",
      "--source",
      "rebelbetting_vb",
      "--run-dir",
      init_payload["run_dir"],
      "--ended-at",
      "2026-03-09T10:20:00Z",
    ],
  )
  assert finalize_result.exit_code == 0

  metadata = json.loads(Path(init_payload["metadata_path"]).read_text())
  event = json.loads(Path(init_payload["events_path"]).read_text())

  assert metadata["ended_at"] == "2026-03-09T10:20:00Z"
  assert metadata["page_count"] == 1
  assert event["kind"] == "dashboard_snapshot"


def test_record_transport_appends_sanitized_event(tmp_path: Path) -> None:
  runner = CliRunner()

  init_result = runner.invoke(
    app,
    [
      "init-run",
      "--source",
      "rebelbetting_vb",
      "--root-dir",
      str(tmp_path),
      "--started-at",
      "2026-03-09T10:15:00Z",
      "--collector-version",
      "test-v1",
      "--browser-profile-used",
      "helium-copy",
      "--transport",
    ],
  )
  assert init_result.exit_code == 0
  init_payload = json.loads(init_result.output)

  payload_path = tmp_path / "transport.json"
  payload_path.write_text(
    json.dumps(
      {
        "type": "ws_handshake_request",
        "headers": {
          "Cookie": "abc=123",
          "Authorization": "Bearer secret-token",
        },
        "preview": '{"access_token":"abc","jwt":"eyJabc.def.ghi"}',
      },
    ),
  )

  record_result = runner.invoke(
    app,
    [
      "record-transport",
      "--source",
      "rebelbetting_vb",
      "--run-dir",
      init_payload["run_dir"],
      "--payload-path",
      str(payload_path),
    ],
  )
  assert record_result.exit_code == 0

  event = json.loads(Path(init_payload["transport_path"]).read_text())

  assert event["headers"]["Cookie"] == "[REDACTED]"
  assert event["headers"]["Authorization"] == "[REDACTED]"
  assert "[REDACTED]" in event["preview"]


def test_record_action_appends_action_event(tmp_path: Path) -> None:
  runner = CliRunner()

  init_result = runner.invoke(
    app,
    [
      "init-run",
      "--source",
      "betway_uk",
      "--root-dir",
      str(tmp_path),
      "--started-at",
      "2026-03-09T20:15:00Z",
      "--collector-version",
      "test-v1",
      "--browser-profile-used",
      "helium-copy",
    ],
  )
  assert init_result.exit_code == 0
  init_payload = json.loads(init_result.output)

  payload_path = tmp_path / "action.json"
  payload_path.write_text(
    json.dumps(
      {
        "captured_at": "2026-03-09T20:16:00Z",
        "page": "confirmation",
        "action": "place_bet",
        "target": "place_bet_button",
        "status": "confirmed",
        "url": "https://betway.com/gb/en/sports/event/16431700?marketGroup=SGP",
        "document_title": "Betway confirmation",
        "body_text": "Bet placed",
        "interactive_snapshot": [],
        "links": [],
        "inputs": {"stake": "10"},
        "visible_actions": ["Done"],
        "resource_hosts": ["betway.com"],
        "local_storage_keys": ["theme"],
        "screenshot_path": "screenshots/confirmation-20260309T201600Z.png",
        "notes": ["free-bet"],
        "metadata": {"selection": "Almeria 2-0"},
      },
    ),
  )

  result = runner.invoke(
    app,
    [
      "record-action",
      "--source",
      "betway_uk",
      "--run-dir",
      init_payload["run_dir"],
      "--payload-path",
      str(payload_path),
    ],
  )
  assert result.exit_code == 0

  event = json.loads(Path(init_payload["events_path"]).read_text())

  assert event["kind"] == "action_snapshot"
  assert event["action"] == "place_bet"
  assert event["status"] == "confirmed"


def test_manual_capture_workflow_preserves_payload_contract(tmp_path: Path) -> None:
  runner = CliRunner()

  init_result = runner.invoke(
    app,
    [
      "init-run",
      "--source",
      "rebelbetting_vb",
      "--root-dir",
      str(tmp_path),
      "--started-at",
      "2026-03-09T10:15:00Z",
      "--collector-version",
      "test-v1",
      "--browser-profile-used",
      "helium-copy",
      "--transport",
    ],
  )
  assert init_result.exit_code == 0
  init_payload = json.loads(init_result.output)

  page_payload = {
    "captured_at": "2026-03-09T10:16:00Z",
    "page": "dashboard",
    "url": "https://vb.rebelbetting.com/",
    "document_title": "Value betting by RebelBetting",
    "body_text": "25 value bets",
    "interactive_snapshot": [{"tag": "A", "text": "Filters"}],
    "links": ["https://vb.rebelbetting.com/filters"],
    "inputs": {"search": ""},
    "visible_actions": ["Filters"],
    "resource_hosts": ["vb.rebelbetting.com"],
    "local_storage_keys": ["Token"],
    "screenshot_path": "/tmp/dashboard.png",
    "notes": ["trial-mode"],
  }
  payload_path = tmp_path / "vb-dashboard.json"
  payload_path.write_text(json.dumps(page_payload))

  record_page_result = runner.invoke(
    app,
    [
      "record-page",
      "--source",
      "rebelbetting_vb",
      "--run-dir",
      init_payload["run_dir"],
      "--payload-path",
      str(payload_path),
    ],
  )
  assert record_page_result.exit_code == 0

  transport_payload = {
    "type": "ws_handshake_request",
    "headers": {
      "Cookie": "abc=123",
      "Authorization": "Bearer secret-token",
    },
    "preview": '{"access_token":"abc","jwt":"eyJabc.def.ghi"}',
  }
  transport_payload_path = tmp_path / "transport.json"
  transport_payload_path.write_text(json.dumps(transport_payload))

  record_transport_result = runner.invoke(
    app,
    [
      "record-transport",
      "--source",
      "rebelbetting_vb",
      "--run-dir",
      init_payload["run_dir"],
      "--payload-path",
      str(transport_payload_path),
    ],
  )
  assert record_transport_result.exit_code == 0

  finalize_result = runner.invoke(
    app,
    [
      "finalize-run",
      "--source",
      "rebelbetting_vb",
      "--run-dir",
      init_payload["run_dir"],
      "--ended-at",
      "2026-03-09T10:20:00Z",
    ],
  )
  assert finalize_result.exit_code == 0

  metadata = json.loads(Path(init_payload["metadata_path"]).read_text())
  events = [
    json.loads(line)
    for line in Path(init_payload["events_path"]).read_text().splitlines()
    if line.strip()
  ]
  transport_events = [
    json.loads(line)
    for line in Path(init_payload["transport_path"]).read_text().splitlines()
    if line.strip()
  ]

  assert len(events) == 1
  assert events[0]["captured_at"] == page_payload["captured_at"]
  assert events[0]["source"] == "rebelbetting_vb"
  assert events[0]["kind"] == "dashboard_snapshot"
  assert events[0]["page"] == page_payload["page"]
  assert events[0]["url"] == page_payload["url"]
  assert events[0]["document_title"] == page_payload["document_title"]
  assert events[0]["body_text"] == page_payload["body_text"]
  assert events[0]["interactive_snapshot"] == page_payload["interactive_snapshot"]
  assert events[0]["links"] == page_payload["links"]
  assert events[0]["inputs"] == page_payload["inputs"]
  assert events[0]["visible_actions"] == page_payload["visible_actions"]
  assert events[0]["resource_hosts"] == page_payload["resource_hosts"]
  assert events[0]["local_storage_keys"] == page_payload["local_storage_keys"]
  assert events[0]["screenshot_path"] == page_payload["screenshot_path"]
  assert events[0]["notes"] == page_payload["notes"]

  assert len(transport_events) == 1
  assert transport_events[0]["headers"]["Cookie"] == "[REDACTED]"
  assert transport_events[0]["headers"]["Authorization"] == "[REDACTED]"
  assert "[REDACTED]" in transport_events[0]["preview"]

  assert metadata["page_count"] == 1
  assert metadata["transport_event_count"] == 1
  assert metadata["ended_at"] == "2026-03-09T10:20:00Z"


def test_extract_page_reports_source_specific_analysis(tmp_path: Path) -> None:
  runner = CliRunner()
  payload_path = tmp_path / "betway-betslip.json"
  payload_path.write_text(
    json.dumps(
      {
        "page": "betslip",
        "body_text": (
          "4 Bet Slip Odds 7/1 Almeria win Under 2.5 Goals Both Teams To Score No "
          "Almeria Team Goals Over 1.5 Free Bet Stake 10 Place Bet"
        ),
        "inputs": {"stake": "10"},
        "visible_actions": ["Place Bet"],
      },
    ),
  )

  result = runner.invoke(
    app,
    [
      "extract-page",
      "--source",
      "betway_uk",
      "--payload-path",
      str(payload_path),
    ],
  )

  assert result.exit_code == 0
  payload = json.loads(result.output)
  assert payload["selection_count"] == 4
  assert payload["free_bet"] is True


def test_calc_trade_out_lay_reports_hedge_and_locked_profit() -> None:
  runner = CliRunner()

  result = runner.invoke(
    app,
    [
      "calc-trade-out-lay",
      "--entry-lay-odds",
      "3.35",
      "--lay-stake",
      "9.91",
      "--current-back-odds",
      "4.8",
      "--commission-rate",
      "0",
    ],
  )

  assert result.exit_code == 0
  payload = json.loads(result.output)
  assert round(payload["hedge_back_stake"], 2) == 6.92
  assert round(payload["locked_profit"], 2) == 2.99


def test_watch_open_positions_reports_grouped_exit_thresholds(tmp_path: Path) -> None:
  runner = CliRunner()
  payload_path = tmp_path / "smarkets-open-positions.json"
  payload_path.write_text(
    json.dumps(
      {
        "page": "open_positions",
        "body_text": (
          "Lazio vs Sassuolo "
          "Sell 1 - 1 Correct score 7.2 £2.55 £15.81 £18.36 £2.46 -£0.09 (3.53%) Order filled Trade out "
          "Sell 1 - 1 Correct score 7.2 £0.41 £2.53 £2.94 £0.32 -£0.09 (22.05%) Order filled Trade out "
          "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£0.31 (3.13%) Order filled Trade out"
        ),
        "inputs": {},
        "visible_actions": ["Trade out"],
      },
    ),
  )

  result = runner.invoke(
    app,
    [
      "watch-open-positions",
      "--payload-path",
      str(payload_path),
      "--commission-rate",
      "0",
      "--target-profit",
      "1",
      "--stop-loss",
      "1",
    ],
  )

  assert result.exit_code == 0
  payload = json.loads(result.output)
  assert payload["watch_count"] == 2
  assert payload["watches"][0]["contract"] == "1 - 1"
  assert round(payload["watches"][0]["profit_take_back_odds"], 2) == 10.87
  assert round(payload["watches"][0]["stop_loss_back_odds"], 2) == 5.38
  assert round(payload["watches"][1]["profit_take_back_odds"], 2) == 3.73
  assert round(payload["watches"][1]["stop_loss_back_odds"], 2) == 3.04


def test_record_watch_plan_appends_watch_plan_event(tmp_path: Path) -> None:
  runner = CliRunner()

  init_result = runner.invoke(
    app,
    [
      "init-run",
      "--source",
      "smarkets_exchange",
      "--root-dir",
      str(tmp_path),
      "--started-at",
      "2026-03-09T20:15:00Z",
      "--collector-version",
      "test-v1",
      "--browser-profile-used",
      "helium-copy",
    ],
  )
  assert init_result.exit_code == 0
  init_payload = json.loads(init_result.output)

  payload_path = tmp_path / "watch-plan.json"
  payload_path.write_text(
    json.dumps(
      {
        "captured_at": "2026-03-09T20:45:00Z",
        "page": "open_positions",
        "commission_rate": 0.0,
        "target_profit": 1.0,
        "stop_loss": 1.0,
        "position_count": 3,
        "watch_count": 2,
        "watches": [
          {
            "contract": "Draw",
            "market": "Full-time result",
            "profit_take_back_odds": 3.73,
            "stop_loss_back_odds": 3.04,
          },
        ],
      },
    ),
  )

  result = runner.invoke(
    app,
    [
      "record-watch-plan",
      "--source",
      "smarkets_exchange",
      "--run-dir",
      init_payload["run_dir"],
      "--payload-path",
      str(payload_path),
    ],
  )

  assert result.exit_code == 0
  event = json.loads(Path(init_payload["events_path"]).read_text())
  assert event["kind"] == "watch_plan_snapshot"
  assert event["page"] == "open_positions"
