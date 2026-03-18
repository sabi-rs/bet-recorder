from pathlib import Path
import sys
import json

from typer.testing import CliRunner

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.browser.cdp import DebugTarget
from bet_recorder.cli import app
from bet_recorder.exchange_worker import (
    WorkerConfig,
    handle_worker_request,
    handle_worker_request_line,
    load_exchange_snapshot_for_config,
    load_historical_positions,
)


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
        .replace('"__RUN_DIR__"', "null")
        .replace('"__ACCOUNT_PAYLOAD_PATH__"', "null")
        .replace('"__OPEN_BETS_PAYLOAD_PATH__"', "null")
        .replace('"__COMPANION_LEGS_PATH__"', "null")
        .replace('"__AGENT_BROWSER_SESSION__"', "null")
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
    assert responses[0]["snapshot"]["runtime"]["source"] == "positions_snapshot"
    assert (
        responses[1]["snapshot"]["status_line"]
        == responses[0]["snapshot"]["status_line"]
    )
    assert responses[1]["snapshot"]["watch"]["watch_count"] == 2
    assert responses[2]["snapshot"]["watch"]["watch_count"] == 2


def test_load_historical_positions_groups_settled_market_pnl_across_matched_entries(
    tmp_path: Path,
) -> None:
    ledger_history_path = tmp_path / "statement-history.json"
    ledger_history_path.write_text(
        json.dumps(
            {
                "ledger_entries": [
                    {
                        "occurred_at": "2026-03-14T19:29:00Z",
                        "platform": "bet10",
                        "activity_type": "bet_settled",
                        "status": "settled",
                        "platform_kind": "sportsbook",
                        "event": "Arsenal vs Everton",
                        "market": "Full-time result",
                        "selection": "Arsenal",
                        "sport_name": "Football",
                        "stake_gbp": 8.54,
                        "odds_decimal": 2.64,
                        "payout_gbp": 22.55,
                        "realised_pnl_gbp": 14.01,
                    },
                    {
                        "occurred_at": "2026-03-14T19:29:00Z",
                        "platform": "smarkets",
                        "activity_type": "market_settled",
                        "status": "settled",
                        "platform_kind": "exchange",
                        "event": "Arsenal vs Everton",
                        "market": "Full-time result",
                        "selection": "",
                        "sport_name": "Football",
                        "amount_gbp": -6.20,
                        "payout_gbp": -6.20,
                        "realised_pnl_gbp": -6.20,
                    },
                ]
            }
        )
    )

    rows = load_historical_positions(ledger_history_path)

    assert len(rows) == 1
    assert rows[0]["event"] == "Arsenal vs Everton"
    assert rows[0]["contract"] == "Arsenal"
    assert rows[0]["market_status"] == "settled"
    assert rows[0]["event_status"] == "Settled"
    assert rows[0]["pnl_amount"] == 7.81


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
    assert response["snapshot"]["runtime"]["source"] == "positions_snapshot"


def test_exchange_worker_session_keeps_running_after_request_error(
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    run_dir = tmp_path / "smarkets-run"
    run_dir.mkdir()
    (run_dir / "events.jsonl").write_text("")

    load_dashboard_request = json.dumps(
        {
            "LoadDashboard": {
                "config": {
                    "positions_payload_path": None,
                    "run_dir": str(run_dir),
                    "account_payload_path": None,
                    "open_bets_payload_path": None,
                    "companion_legs_path": None,
                    "agent_browser_session": None,
                    "commission_rate": 0.0,
                    "target_profit": 1.0,
                    "stop_loss": 1.0,
                    "hard_margin_call_profit_floor": None,
                    "warn_only_default": True,
                },
            },
        },
    )

    result = runner.invoke(
        app,
        ["exchange-worker-session"],
        input=f"{load_dashboard_request}\n{json.dumps('Refresh')}\n",
    )

    assert result.exit_code == 0
    responses = [json.loads(line) for line in result.output.splitlines()]
    assert len(responses) == 2
    assert responses[0]["request_error"].startswith(
        "No positions_snapshot event found in run bundle"
    )
    assert responses[1]["request_error"].startswith(
        "No positions_snapshot event found in run bundle"
    )


def test_handle_worker_request_line_preserves_config_after_request_error(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "smarkets-run"
    run_dir.mkdir()
    events_path = run_dir / "events.jsonl"
    events_path.write_text("")

    request_line = json.dumps(
        {
            "LoadDashboard": {
                "config": {
                    "positions_payload_path": None,
                    "run_dir": str(run_dir),
                    "account_payload_path": None,
                    "open_bets_payload_path": None,
                    "companion_legs_path": None,
                    "agent_browser_session": None,
                    "commission_rate": 0.0,
                    "target_profit": 1.0,
                    "stop_loss": 1.0,
                    "hard_margin_call_profit_floor": None,
                    "warn_only_default": True,
                },
            },
        },
    )

    error_response, resolved_config = handle_worker_request_line(
        request_line=request_line,
        config=None,
    )

    assert error_response["request_error"].startswith(
        "No positions_snapshot event found in run bundle"
    )
    assert resolved_config is not None
    assert resolved_config.run_dir == run_dir

    events_path.write_text(
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

    success_response, refreshed_config = handle_worker_request_line(
        request_line=json.dumps("Refresh"),
        config=resolved_config,
    )

    assert refreshed_config == resolved_config
    assert "request_error" not in success_response
    assert success_response["snapshot"]["watch"]["watch_count"] == 1


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
                    "(3.13%) Order filled Trade out Back 5.00"
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
            companion_legs_path=None,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
            hard_margin_call_profit_floor=None,
            warn_only_default=True,
            agent_browser_session="helium-copy",
        ),
    )

    assert captured["session"] == "helium-copy"
    assert snapshot["account_stats"]["available_balance"] == 150.0
    assert snapshot["runtime"]["source"] == "positions_snapshot"


def test_refresh_request_uses_run_dir_state_without_live_recap(
    tmp_path: Path,
    monkeypatch,
) -> None:
    run_dir = tmp_path / "smarkets-run"
    run_dir.mkdir()
    (run_dir / "events.jsonl").write_text(
        json.dumps(
            {
                "captured_at": "2026-03-11T11:05:00Z",
                "source": "smarkets_exchange",
                "kind": "positions_snapshot",
                "page": "open_positions",
                "url": "https://smarkets.com/portfolio/",
                "document_title": "Open positions",
                "body_text": (
                    "Available balance £150.00 Exposure £23.29 Unrealized P/L £2.10 "
                    "Lazio vs Sassuolo "
                    "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£0.31 "
                    "(3.13%) Order filled Trade out Back 4.80"
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
    (run_dir / "watcher-state.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-11T11:05:00Z",
                "interval_seconds": 5.0,
                "iteration": 9,
                "worker": {
                    "name": "bet-recorder",
                    "status": "ready",
                    "detail": "watcher stable",
                },
                "session": {
                    "name": "helium-copy",
                    "current_url": "https://smarkets.com/portfolio/",
                    "document_title": "Open positions",
                    "page_hint": "open_positions",
                    "open_positions_ready": True,
                    "validation_error": None,
                },
                "decision_count": 1,
                "decisions": [{"contract": "Draw", "status": "hold"}],
            },
        )
        + "\n",
    )

    capture_count = 0

    def fake_capture(config: WorkerConfig) -> None:
        nonlocal capture_count
        capture_count += 1
        if capture_count > 1:
            raise AssertionError("Refresh should not recapture live browser state")

    monkeypatch.setattr(
        "bet_recorder.exchange_worker.capture_current_smarkets_open_positions",
        fake_capture,
    )

    config = WorkerConfig(
        positions_payload_path=None,
        run_dir=run_dir,
        account_payload_path=None,
        open_bets_payload_path=None,
        companion_legs_path=None,
        commission_rate=0.0,
        target_profit=1.0,
        stop_loss=1.0,
        hard_margin_call_profit_floor=None,
        warn_only_default=True,
        agent_browser_session="helium-copy",
    )

    first_response, resolved_config = handle_worker_request(
        request={"LoadDashboard": {"config": worker_config_payload(config)}},
        config=None,
    )
    refresh_response, _ = handle_worker_request(
        request="Refresh",
        config=resolved_config,
    )

    assert capture_count == 1
    assert first_response["snapshot"]["runtime"]["source"] == "watcher-state"
    assert refresh_response["snapshot"]["runtime"]["source"] == "watcher-state"
    assert (
        refresh_response["snapshot"]["runtime"]["session"]["current_url"]
        == "https://smarkets.com/portfolio/"
    )


def test_load_exchange_snapshot_uses_watcher_state_runtime_when_available(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "smarkets-run"
    run_dir.mkdir()
    (run_dir / "events.jsonl").write_text(
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
                    "Lazio vs Sassuolo "
                    "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£0.31 "
                    "(3.13%) Order filled Trade out Back 4.80"
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
    (run_dir / "watcher-state.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-11T11:05:00Z",
                "interval_seconds": 5.0,
                "iteration": 9,
                "decision_count": 1,
                "decisions": [{"contract": "Draw", "status": "take_profit_ready"}],
            },
        )
        + "\n",
    )

    snapshot = load_exchange_snapshot_for_config(
        WorkerConfig(
            positions_payload_path=None,
            run_dir=run_dir,
            account_payload_path=None,
            open_bets_payload_path=None,
            companion_legs_path=None,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
            hard_margin_call_profit_floor=None,
            warn_only_default=True,
            agent_browser_session=None,
        ),
    )

    assert snapshot["runtime"]["source"] == "watcher-state"
    assert snapshot["runtime"]["watcher_iteration"] == 9
    assert snapshot["runtime"]["decision_count"] == 1


def test_load_exchange_snapshot_prefers_ready_watcher_state_positions(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "smarkets-run"
    run_dir.mkdir()
    (run_dir / "events.jsonl").write_text(
        json.dumps(
            {
                "captured_at": "2026-03-11T11:05:00Z",
                "source": "smarkets_exchange",
                "kind": "positions_snapshot",
                "page": "open_positions",
                "url": "https://smarkets.com/open-positions",
                "document_title": "Open positions",
                "body_text": (
                    "West Ham vs Man City "
                    "27'|Premier League "
                    "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£0.31 "
                    "(3.13%) Order filled Trade out Back 2.80"
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
    (run_dir / "watcher-state.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-11T11:05:00Z",
                "interval_seconds": 5.0,
                "iteration": 9,
                "worker": {
                    "name": "bet-recorder",
                    "status": "ready",
                    "detail": "Watcher iteration captured 1 watch groups from 1 positions.",
                },
                "account_stats": None,
                "open_positions": [
                    {
                        "event": "West Ham vs Man City",
                        "event_status": "27'|Premier League",
                        "event_url": "https://smarkets.com/football/england-premier-league/2026/03/14/20-00/west-ham-vs-manchester-city/44919693/",
                        "contract": "Draw",
                        "market": "Full-time result",
                        "status": "Order filled",
                        "market_status": "tradable",
                        "is_in_play": True,
                        "price": 3.35,
                        "stake": 9.91,
                        "liability": 23.29,
                        "current_value": 9.60,
                        "pnl_amount": -0.31,
                        "current_back_odds": 2.80,
                        "current_implied_probability": 1 / 2.80,
                        "current_implied_percentage": 100 / 2.80,
                        "current_score": "0-0",
                        "current_score_home": 0,
                        "current_score_away": 0,
                        "can_trade_out": True,
                    }
                ],
                "other_open_bets": [],
                "watch": {
                    "position_count": 1,
                    "watch_count": 1,
                    "commission_rate": 0.0,
                    "target_profit": 1.0,
                    "stop_loss": 1.0,
                    "watches": [],
                },
                "decision_count": 1,
                "decisions": [{"contract": "Draw", "status": "hold"}],
            },
        )
        + "\n",
    )

    snapshot = load_exchange_snapshot_for_config(
        WorkerConfig(
            positions_payload_path=None,
            run_dir=run_dir,
            account_payload_path=None,
            open_bets_payload_path=None,
            companion_legs_path=None,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
            hard_margin_call_profit_floor=None,
            warn_only_default=True,
            agent_browser_session=None,
        ),
    )

    assert snapshot["runtime"]["source"] == "watcher-state"
    assert snapshot["worker"]["status"] == "ready"
    assert snapshot["open_positions"][0]["current_score"] == "0-0"
    assert snapshot["open_positions"][0]["event_url"].endswith("/44919693/")


def test_exchange_worker_session_reports_session_page_diagnostics(
    tmp_path: Path,
    monkeypatch,
) -> None:
    run_dir = tmp_path / "smarkets-run"
    run_dir.mkdir()
    (run_dir / "events.jsonl").write_text("")

    def fake_capture(config: WorkerConfig) -> None:
        (run_dir / "watcher-state.json").write_text(
            json.dumps(
                {
                    "updated_at": "2026-03-11T11:05:00Z",
                    "interval_seconds": 5.0,
                    "iteration": 1,
                    "worker": {
                        "name": "bet-recorder",
                        "status": "error",
                        "detail": 'Agent-browser session "helium-copy" is not ready: url=about:blank title="Blank page".',
                    },
                    "session": {
                        "name": "helium-copy",
                        "current_url": "about:blank",
                        "document_title": "Blank page",
                        "page_hint": "unknown",
                        "open_positions_ready": False,
                        "validation_error": 'Agent-browser session "helium-copy" is not ready: url=about:blank title="Blank page".',
                    },
                    "decision_count": 0,
                    "decisions": [],
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
            companion_legs_path=None,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
            hard_margin_call_profit_floor=None,
            warn_only_default=True,
            agent_browser_session="helium-copy",
        ),
    )

    assert snapshot["worker"]["status"] == "error"
    assert snapshot["runtime"]["session"]["current_url"] == "about:blank"
    assert snapshot["runtime"]["session"]["document_title"] == "Blank page"
    assert snapshot["runtime"]["session"]["open_positions_ready"] is False
    assert "not ready" in snapshot["runtime"]["session"]["validation_error"]


def test_exchange_worker_session_returns_ledger_sections(
    tmp_path: Path,
) -> None:
    positions_payload_path = tmp_path / "smarkets-open-positions.json"
    positions_payload_path.write_text(
        json.dumps(
            {
                "page": "open_positions",
                "body_text": (
                    "Available balance £120.45 Exposure £41.63 Unrealized P/L -£0.49 "
                    "Open Bets Back Arsenal Full-time result 2.12 £5.00 Open "
                    "Lazio vs Sassuolo "
                    "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£0.31 "
                    "(3.13%) Order filled Trade out Back 5.00"
                ),
                "inputs": {},
                "visible_actions": ["Trade out"],
            },
        ),
    )

    snapshot = load_exchange_snapshot_for_config(
        WorkerConfig(
            positions_payload_path=positions_payload_path,
            run_dir=None,
            account_payload_path=None,
            open_bets_payload_path=None,
            companion_legs_path=None,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
            hard_margin_call_profit_floor=None,
            warn_only_default=True,
            agent_browser_session=None,
        ),
    )

    assert snapshot["tracked_bets"] == []
    assert snapshot["exit_policy"]["target_profit"] == 1.0
    assert snapshot["exit_policy"]["stop_loss"] == 1.0
    assert snapshot["exit_policy"]["warn_only_default"] is True
    assert snapshot["exit_recommendations"] == []


def test_exchange_worker_session_merges_companion_legs_into_tracked_bets(
    tmp_path: Path,
) -> None:
    positions_payload_path = tmp_path / "smarkets-open-positions.json"
    positions_payload_path.write_text(
        json.dumps(
            {
                "page": "open_positions",
                "body_text": (
                    "Available balance £120.45 Exposure £41.63 Unrealized P/L -£0.49 "
                    "Open Bets Back Arsenal Full-time result 2.12 £5.00 Open "
                    "Lazio vs Sassuolo "
                    "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£0.31 "
                    "(3.13%) Order filled Trade out Back 5.00"
                ),
                "inputs": {},
                "visible_actions": ["Trade out"],
            },
        ),
    )
    companion_legs_path = tmp_path / "companion-legs.json"
    companion_legs_path.write_text(
        json.dumps(
            {
                "tracked_bets": [
                    {
                        "bet_id": "bet-001",
                        "group_id": "group-arsenal-everton",
                        "placed_at": "2026-03-13T10:30:00Z",
                        "platform": "bet365",
                        "sport_key": "soccer_epl",
                        "sport_name": "Premier League",
                        "event": "Arsenal v Everton",
                        "market": "Full-time result",
                        "selection": "Draw",
                        "status": "open",
                        "stake_gbp": 2.0,
                        "odds_reference": {"fair_price": 2.4},
                        "legs": [
                            {
                                "venue": "smarkets",
                                "outcome": "Draw",
                                "side": "lay",
                                "odds": 3.35,
                                "stake": 9.91,
                                "status": "open",
                                "commission_rate": 0.0,
                            },
                            {
                                "venue": "bet365",
                                "outcome": "Draw",
                                "side": "back",
                                "odds": 2.12,
                                "stake": 2.0,
                                "status": "matched",
                            },
                        ],
                    }
                ]
            },
        ),
    )

    snapshot = load_exchange_snapshot_for_config(
        WorkerConfig(
            positions_payload_path=positions_payload_path,
            run_dir=None,
            account_payload_path=None,
            open_bets_payload_path=None,
            companion_legs_path=companion_legs_path,
            commission_rate=0.0,
            target_profit=5.0,
            stop_loss=5.0,
            hard_margin_call_profit_floor=None,
            warn_only_default=True,
            agent_browser_session=None,
        ),
    )

    assert snapshot["open_positions"][0]["contract"] == "Draw"
    assert snapshot["tracked_bets"][0]["bet_id"] == "bet-001"
    assert snapshot["tracked_bets"][0]["platform"] == "bet365"
    assert snapshot["tracked_bets"][0]["exchange"] == "smarkets"
    assert snapshot["tracked_bets"][0]["sport_key"] == "soccer_epl"
    assert snapshot["tracked_bets"][0]["market_family"] == "match_odds"
    assert snapshot["tracked_bets"][0]["expected_ev"]["status"] == "calculated"
    assert snapshot["tracked_bets"][0]["legs"][1]["venue"] == "bet365"
    assert snapshot["exit_recommendations"][0]["action"] == "hold"
    assert snapshot["exit_recommendations"][0]["reason"] == "within_thresholds"


def test_load_exchange_snapshot_surfaces_watcher_error_state(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "smarkets-run"
    run_dir.mkdir()
    (run_dir / "events.jsonl").write_text("")
    (run_dir / "watcher-state.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-11T11:05:00Z",
                "interval_seconds": 5.0,
                "iteration": 9,
                "worker": {
                    "name": "bet-recorder",
                    "status": "error",
                    "detail": 'Agent-browser session "helium-copy" is on about:blank, not Smarkets open positions.',
                },
                "watch": {
                    "position_count": 0,
                    "watch_count": 0,
                    "commission_rate": 0.0,
                    "target_profit": 0.0,
                    "stop_loss": 0.0,
                    "watches": [],
                },
                "decision_count": 0,
                "decisions": [],
            },
        )
        + "\n",
    )

    snapshot = load_exchange_snapshot_for_config(
        WorkerConfig(
            positions_payload_path=None,
            run_dir=run_dir,
            account_payload_path=None,
            open_bets_payload_path=None,
            companion_legs_path=None,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
            hard_margin_call_profit_floor=None,
            warn_only_default=True,
            agent_browser_session=None,
        ),
    )

    assert snapshot["worker"]["status"] == "error"
    assert "about:blank" in snapshot["status_line"]
    assert snapshot["venues"][0]["status"] == "error"
    assert snapshot["runtime"]["source"] == "watcher-state"


def test_load_exchange_snapshot_supports_bet365_live_browser_payload(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "bet_recorder.exchange_worker.capture_current_live_venue_payload",
        lambda venue: {
            "page": "my_bets",
            "url": "https://www.bet365.com/#/MB/UB",
            "document_title": "bet365 My Bets",
            "body_text": (
                "All Sports\nIn-Play\nMy Bets\n1\nOpen\nCash Out\nLive\nSettled\n"
                "£10.00\nSingle\nBrumbies\nReuse Selections\nBrumbies\n3.10\nTo Win\n"
                "Brumbies\nFri 20 Mar\nChiefs\n08:35\nStake\n£10.00\n"
                "£10.00 Bet Credits\nNet Return\n£21.00"
            ),
            "inputs": {},
            "visible_actions": ["My Bets", "Cash Out"],
            "captured_at": "2026-03-18T08:15:00Z",
        },
    )
    monkeypatch.setattr(
        "bet_recorder.exchange_worker.list_debug_targets",
        lambda: [
            DebugTarget(
                target_id="bet365-live",
                target_type="page",
                title="bet365 My Bets",
                url="https://www.bet365.com/#/MB/UB",
                websocket_debugger_url="ws://example.invalid/bet365",
            )
        ],
    )

    snapshot = load_exchange_snapshot_for_config(
        WorkerConfig(
            positions_payload_path=Path("/tmp/unused-positions.json"),
            run_dir=None,
            account_payload_path=None,
            open_bets_payload_path=None,
            companion_legs_path=None,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
            hard_margin_call_profit_floor=None,
            warn_only_default=True,
            agent_browser_session=None,
        ),
        selected_venue="bet365",
    )

    assert snapshot["selected_venue"] == "bet365"
    assert snapshot["worker"]["status"] == "ready"
    assert snapshot["other_open_bets"][0]["label"] == "Brumbies"
    assert snapshot["other_open_bets"][0]["odds"] == 3.1
    assert snapshot["venues"][1]["id"] == "bet365"
    assert snapshot["venues"][1]["status"] == "ready"


def test_handle_worker_request_line_persists_selected_live_venue(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "bet_recorder.exchange_worker.capture_current_live_venue_payload",
        lambda venue: {
            "page": "my_bets",
            "url": "https://www.bet365.com/#/MB/UB",
            "document_title": "bet365 My Bets",
            "body_text": "+0\nMy Bets\n",
            "inputs": {},
            "visible_actions": ["My Bets"],
            "captured_at": "2026-03-18T08:15:00Z",
        },
    )
    monkeypatch.setattr(
        "bet_recorder.exchange_worker.list_debug_targets",
        lambda: [
            DebugTarget(
                target_id="bet365-live",
                target_type="page",
                title="bet365 My Bets",
                url="https://www.bet365.com/#/MB/UB",
                websocket_debugger_url="ws://example.invalid/bet365",
            )
        ],
    )
    config = WorkerConfig(
        positions_payload_path=Path("/tmp/unused-positions.json"),
        run_dir=None,
        account_payload_path=None,
        open_bets_payload_path=None,
        companion_legs_path=None,
        commission_rate=0.0,
        target_profit=1.0,
        stop_loss=1.0,
        hard_margin_call_profit_floor=None,
        warn_only_default=True,
        agent_browser_session=None,
    )

    response, next_config, next_selected_venue = handle_worker_request_line(
        request_line=json.dumps({"SelectVenue": {"venue": "bet365"}}),
        config=config,
        selected_venue="smarkets",
    )

    assert next_config == config
    assert next_selected_venue == "bet365"
    assert response["snapshot"]["selected_venue"] == "bet365"


def test_load_exchange_snapshot_handles_missing_live_tab_gracefully() -> None:
    snapshot = load_exchange_snapshot_for_config(
        WorkerConfig(
            positions_payload_path=Path("/tmp/unused-positions.json"),
            run_dir=None,
            account_payload_path=None,
            open_bets_payload_path=None,
            companion_legs_path=None,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
            hard_margin_call_profit_floor=None,
            warn_only_default=True,
            agent_browser_session=None,
        ),
        selected_venue="betfred",
    )

    assert snapshot["selected_venue"] == "betfred"
    assert snapshot["other_open_bets"] == []
    assert snapshot["worker"]["status"] == "error"
    assert "Could not find a CDP target" in snapshot["status_line"]


def fixture_text(name: str) -> str:
    return (Path(__file__).resolve().parent / "fixtures" / "worker" / name).read_text()


def worker_config_payload(config: WorkerConfig) -> dict:
    return {
        "positions_payload_path": (
            str(config.positions_payload_path)
            if config.positions_payload_path is not None
            else None
        ),
        "run_dir": str(config.run_dir) if config.run_dir is not None else None,
        "account_payload_path": (
            str(config.account_payload_path)
            if config.account_payload_path is not None
            else None
        ),
        "open_bets_payload_path": (
            str(config.open_bets_payload_path)
            if config.open_bets_payload_path is not None
            else None
        ),
        "companion_legs_path": (
            str(config.companion_legs_path)
            if config.companion_legs_path is not None
            else None
        ),
        "agent_browser_session": config.agent_browser_session,
        "commission_rate": config.commission_rate,
        "target_profit": config.target_profit,
        "stop_loss": config.stop_loss,
        "hard_margin_call_profit_floor": config.hard_margin_call_profit_floor,
        "warn_only_default": config.warn_only_default,
    }
