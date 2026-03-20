from pathlib import Path
import sys
import json
from urllib.error import URLError

import pytest
from typer.testing import CliRunner

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.browser.cdp import DebugTarget
from bet_recorder.cli import app
from bet_recorder import exchange_worker as exchange_worker_module
from bet_recorder.exchange_worker import (
    WorkerConfig,
    handle_worker_request,
    handle_worker_request_line,
    load_exchange_snapshot_for_config,
    load_historical_positions,
    load_ledger_pnl_summary,
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
                json.dumps("RefreshCached"),
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


def test_capture_live_horse_market_snapshot_prefers_relevant_targets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    targets = [
        DebugTarget(
            target_id="football",
            target_type="page",
            title="Smarkets Predictions",
            url="https://smarkets.com/football/premier-league",
            websocket_debugger_url="ws://football",
        ),
        DebugTarget(
            target_id="horse",
            target_type="page",
            title="Smarkets Predictions",
            url="https://smarkets.com/horse-racing/newbury/2026/03/20/15-32/44958026/",
            websocket_debugger_url="ws://horse",
        ),
    ]
    seen_targets: list[str] = []

    monkeypatch.setattr(exchange_worker_module, "list_debug_targets", lambda: targets)

    def fake_capture_debug_target_page_state(**kwargs):
        seen_targets.append(kwargs["websocket_debugger_url"])
        return {
            "page": "market",
            "url": "https://smarkets.com/horse-racing/newbury/2026/03/20/15-32/44958026/",
            "document_title": "15:32 Newbury - Win Market",
            "body_text": "15:32 Newbury Winner Desert Hero 5.2 King Of Steel 6.0",
            "interactive_snapshot": [],
            "captured_at": "2026-03-20T12:00:00Z",
        }

    monkeypatch.setattr(
        exchange_worker_module,
        "capture_debug_target_page_state",
        fake_capture_debug_target_page_state,
    )
    monkeypatch.setattr(
        exchange_worker_module,
        "analyze_racing_market_page",
        lambda **_: {
            "status": "ready",
            "detail": "Captured 2 smarkets racing quote(s).",
            "event_name": "15:32 Newbury",
            "market_name": "Win",
            "start_hint": "15:32",
            "quotes": [
                {"selection_name": "Desert Hero", "side": "lay", "odds": 5.4},
                {"selection_name": "King Of Steel", "side": "lay", "odds": 6.0},
            ],
        },
    )

    snapshot = exchange_worker_module.capture_live_horse_market_snapshot(
        {"bookmakers": [], "exchanges": ["smarkets"], "search": []}
    )

    assert seen_targets == ["ws://horse"]
    assert snapshot["ready_source_count"] == 1
    assert snapshot["sources"][0]["status"] == "ready"


def test_capture_live_horse_market_snapshot_records_target_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    targets = [
        DebugTarget(
            target_id="horse",
            target_type="page",
            title="Smarkets Predictions",
            url="https://smarkets.com/horse-racing/newbury/2026/03/20/15-32/44958026/",
            websocket_debugger_url="ws://horse",
        )
    ]

    monkeypatch.setattr(exchange_worker_module, "list_debug_targets", lambda: targets)
    monkeypatch.setattr(
        exchange_worker_module,
        "capture_debug_target_page_state",
        lambda **_: (_ for _ in ()).throw(ValueError("CDP evaluation timed out after 5s")),
    )

    snapshot = exchange_worker_module.capture_live_horse_market_snapshot(
        {"bookmakers": [], "exchanges": ["smarkets"], "search": []}
    )

    assert snapshot["ready_source_count"] == 0
    assert snapshot["sources"][0]["status"] == "error"
    assert "timed out" in snapshot["sources"][0]["detail"]


def test_exchange_worker_session_attaches_recorder_bundle_evidence(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "events.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "captured_at": "2026-03-20T11:58:00Z",
                        "source": "smarkets_exchange",
                        "kind": "positions_snapshot",
                        "page": "open_positions",
                        "url": "https://smarkets.com/event/1",
                        "document_title": "Open positions",
                        "body_text": (
                            "Available balance £120.45 Exposure £41.63 Unrealized P/L -£0.49 "
                            "Lazio vs Sassuolo "
                            "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£0.31 (3.13%) "
                            "Order filled Trade out"
                        ),
                        "interactive_snapshot": [],
                        "links": [],
                        "inputs": {},
                        "visible_actions": ["Trade out"],
                        "resource_hosts": [],
                        "local_storage_keys": [],
                        "screenshot_path": None,
                        "notes": ["seed snapshot"],
                    }
                ),
                json.dumps(
                    {
                        "captured_at": "2026-03-20T11:59:00Z",
                        "source": "smarkets_exchange",
                        "kind": "watch_plan_snapshot",
                        "page": "open_positions",
                        "commission_rate": 0.0,
                        "target_profit": 1.0,
                        "stop_loss": 1.0,
                        "position_count": 1,
                        "watch_count": 1,
                        "watches": [],
                    }
                ),
                json.dumps(
                    {
                        "captured_at": "2026-03-20T11:59:45Z",
                        "source": "operator_console",
                        "kind": "operator_interaction",
                        "page": "worker_request",
                        "action": "place_bet",
                        "status": "response:submitted",
                        "request_id": "req-1",
                        "reference_id": "bet-1",
                        "detail": "loaded in review mode",
                    }
                ),
                json.dumps(
                    {
                        "captured_at": "2026-03-20T12:00:00Z",
                        "source": "smarkets_exchange",
                        "kind": "action_snapshot",
                        "page": "betslip",
                        "action": "place_order",
                        "target": "Draw",
                        "status": "submitted",
                        "url": "https://smarkets.com/betslip",
                        "document_title": "Betslip",
                        "body_text": "",
                        "interactive_snapshot": [],
                        "links": [],
                        "inputs": {},
                        "visible_actions": [],
                        "resource_hosts": [],
                        "local_storage_keys": [],
                        "screenshot_path": None,
                        "notes": ["submitted from console"],
                        "metadata": {},
                    }
                ),
            ]
        )
        + "\n"
    )
    (run_dir / "transport.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "captured_at": "2026-03-20T11:59:30Z",
                        "kind": "interaction_marker",
                        "action": "place_bet",
                        "phase": "request",
                        "request_id": "req-1",
                        "reference_id": "bet-1",
                        "detail": "review buy Draw stake 10.00 at 5.00",
                    }
                ),
                json.dumps(
                    {
                        "captured_at": "2026-03-20T12:00:01Z",
                        "kind": "interaction_marker",
                        "action": "place_bet",
                        "phase": "response",
                        "request_id": "req-1",
                        "reference_id": "bet-1",
                        "detail": "loaded in review mode",
                    }
                ),
            ]
        )
        + "\n"
    )

    response, resolved_config = handle_worker_request(
        request="LoadDashboard",
        config=WorkerConfig(
            positions_payload_path=None,
            run_dir=run_dir,
            account_payload_path=None,
            open_bets_payload_path=None,
            companion_legs_path=None,
            agent_browser_session=None,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
            hard_margin_call_profit_floor=None,
            warn_only_default=True,
        ),
    )

    assert resolved_config.run_dir == run_dir
    snapshot = response["snapshot"]
    assert snapshot["recorder_bundle"]["event_count"] == 4
    assert snapshot["recorder_bundle"]["latest_event_kind"] == "action_snapshot"
    assert snapshot["recorder_bundle"]["latest_event_at"] == "2026-03-20T12:00:00Z"
    assert snapshot["recorder_events"][0]["kind"] == "action_snapshot"
    assert snapshot["recorder_events"][0]["summary"] == "place_order Draw -> submitted"
    assert snapshot["recorder_events"][1]["kind"] == "operator_interaction"
    assert snapshot["recorder_events"][1]["request_id"] == "req-1"
    assert snapshot["recorder_events"][1]["reference_id"] == "bet-1"
    assert snapshot["recorder_events"][1]["status"] == "response:submitted"
    assert snapshot["recorder_events"][2]["kind"] == "watch_plan_snapshot"
    assert "Watch plan refreshed" in snapshot["recorder_events"][2]["summary"]
    assert snapshot["recorder_events"][3]["kind"] == "positions_snapshot"
    assert snapshot["transport_summary"]["marker_count"] == 2
    assert snapshot["transport_summary"]["latest_marker_phase"] == "response"
    assert snapshot["transport_events"][0]["phase"] == "response"
    assert snapshot["transport_events"][0]["request_id"] == "req-1"
    assert snapshot["transport_events"][1]["phase"] == "request"


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


def test_load_historical_positions_keeps_distinct_bets_separate_when_ids_differ(
    tmp_path: Path,
) -> None:
    ledger_history_path = tmp_path / "statement-history.json"
    ledger_history_path.write_text(
        json.dumps(
            {
                "ledger_entries": [
                    {
                        "occurred_at": "2026-03-14T19:29:00Z",
                        "bet_id": "bet-001",
                        "group_id": "group-001",
                        "platform": "bet365",
                        "activity_type": "bet_settled",
                        "status": "settled",
                        "platform_kind": "sportsbook",
                        "event": "Arsenal vs Everton",
                        "market": "Full-time result",
                        "selection": "Arsenal",
                        "stake_gbp": 8.54,
                        "odds_decimal": 2.64,
                        "payout_gbp": 22.55,
                        "realised_pnl_gbp": 14.01,
                    },
                    {
                        "occurred_at": "2026-03-14T19:29:00Z",
                        "bet_id": "bet-002",
                        "group_id": "group-002",
                        "platform": "betfred",
                        "activity_type": "bet_settled",
                        "status": "settled",
                        "platform_kind": "sportsbook",
                        "event": "Arsenal vs Everton",
                        "market": "Full-time result",
                        "selection": "Draw",
                        "stake_gbp": 5.0,
                        "odds_decimal": 3.4,
                        "payout_gbp": 0.0,
                        "realised_pnl_gbp": -5.0,
                    },
                ]
            }
        )
    )

    rows = load_historical_positions(ledger_history_path)

    assert len(rows) == 2
    assert [row["contract"] for row in rows] == ["Arsenal", "Draw"]


def test_load_ledger_pnl_summary_uses_raw_ledger_totals_and_builds_points(
    tmp_path: Path,
) -> None:
    ledger_history_path = tmp_path / "statement-history.json"
    ledger_history_path.write_text(
        json.dumps(
            {
                "ledger_entries": [
                    {
                        "entry_id": "smarkets:1",
                        "occurred_at": "2026-03-02T10:00:00",
                        "platform": "smarkets",
                        "platform_kind": "exchange",
                        "activity_type": "market_settled",
                        "description": "Market Settled",
                        "realised_pnl_gbp": 10.0,
                    },
                    {
                        "entry_id": "bet10:1",
                        "occurred_at": "2026-03-02T11:00:00",
                        "platform": "bet10",
                        "platform_kind": "sportsbook",
                        "activity_type": "bet_settled",
                        "description": "Lost",
                        "realised_pnl_gbp": -2.0,
                    },
                    {
                        "entry_id": "betfair:bonus",
                        "occurred_at": "2026-03-02T12:00:00",
                        "platform": "betfair",
                        "platform_kind": "sportsbook",
                        "activity_type": "bet_settled",
                        "description": "Free Bet Winner",
                        "realised_pnl_gbp": 4.5,
                    },
                ]
            }
        )
    )

    summary = load_ledger_pnl_summary(ledger_history_path)

    assert summary["realised_total"] == 12.5
    assert summary["exchange_total"] == 10.0
    assert summary["sportsbook_total"] == 2.5
    assert summary["promo_total"] == 4.5
    assert summary["settled_count"] == 3
    assert summary["standard_count"] == 2
    assert summary["promo_count"] == 1
    assert summary["unknown_count"] == 0
    assert [point["total"] for point in summary["points"]] == [10.0, 8.0, 12.5]


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


def test_load_exchange_snapshot_backfills_missing_event_context_from_latest_payload(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "smarkets-run"
    run_dir.mkdir()
    (run_dir / "events.jsonl").write_text(
        json.dumps(
            {
                "captured_at": "2026-03-18T19:43:15Z",
                "source": "smarkets_exchange",
                "kind": "positions_snapshot",
                "page": "open_positions",
                "url": "https://smarkets.com/portfolio/?order-state=active",
                "document_title": "Smarkets Predictions",
                "body_text": (
                    "/\n"
                    "Deposit\n"
                    "Balance\n"
                    "£0.00\n"
                    "TD\n"
                    "Portfolio\n"
                    "Tottenham vs Atlético Madrid\n"
                    "1\n"
                    "In 44 Minutes|UEFA Champions League\n"
                    "-£14.60\n"
                    "Worst Outcome\n"
                    "£10.00\n"
                    "Best Outcome\n"
                    "Contract\n"
                    "Price\n"
                    "Stake/\n"
                    "Liability\n"
                    "Return\n"
                    "Current Value\n"
                    "Status\n"
                    "Sell Tottenham\n"
                    "Full-time result\n"
                    "2.46\n"
                    "£10.00\n"
                    "£14.60\n"
                    "£24.60\n"
                    "£10.08\n"
                    "+£0.08 (0.8%)\n"
                    "Order filled\n"
                    "Trade out\n"
                    "Brumbies vs Chiefs\n"
                    "1\n"
                    "20 Mar 8:35 AM|Super Rugby\n"
                    "-£20.80\n"
                    "Worst Outcome\n"
                    "£10.40\n"
                    "Best Outcome\n"
                    "Contract\n"
                    "Price\n"
                    "Stake/\n"
                    "Liability\n"
                    "Return\n"
                    "Current Value\n"
                    "Status\n"
                    "Sell Brumbies\n"
                    "Winner (including overtime)\n"
                    "3.00\n"
                    "£10.40\n"
                    "£20.80\n"
                    "£31.20\n"
                    "£7.23\n"
                    "-£3.17 (30.48%)\n"
                    "Order filled\n"
                    "Trade out"
                ),
                "interactive_snapshot": [],
                "links": [
                    "https://smarkets.com/football/uefa-champions-league/2026/03/18/20-00/tottenham-hotspur-vs-atletico-de-madrid/44941563/",
                    "https://smarkets.com/rugby/super-rugby/2026/03/20/08-35/brumbies-vs-chiefs/44907713/",
                ],
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
                "source": "smarkets_exchange",
                "run_dir": str(run_dir),
                "updated_at": "2026-03-18T19:43:20Z",
                "interval_seconds": 5,
                "iteration": 42,
                "worker": {
                    "name": "bet-recorder",
                    "status": "ready",
                    "detail": "Watcher iteration captured 2 watch groups from 2 positions.",
                },
                "account_stats": None,
                "open_positions": [
                    {
                        "event": "",
                        "event_status": "",
                        "event_url": "",
                        "contract": "Tottenham",
                        "market": "Full-time result",
                        "status": "filled",
                        "market_status": "tradable",
                        "is_in_play": False,
                        "price": 2.46,
                        "stake": 10.0,
                        "liability": 14.6,
                        "current_value": 10.08,
                        "pnl_amount": 0.08,
                        "live_clock": "",
                        "can_trade_out": True,
                    },
                    {
                        "event": "",
                        "event_status": "",
                        "event_url": "",
                        "contract": "Brumbies",
                        "market": "Winner (including overtime)",
                        "status": "filled",
                        "market_status": "tradable",
                        "is_in_play": False,
                        "price": 3.0,
                        "stake": 10.4,
                        "liability": 20.8,
                        "current_value": 7.23,
                        "pnl_amount": -3.17,
                        "live_clock": "",
                        "can_trade_out": True,
                    },
                ],
                "other_open_bets": [],
                "watch": {
                    "position_count": 2,
                    "watch_count": 2,
                    "commission_rate": 0.0,
                    "target_profit": 1.0,
                    "stop_loss": 1.0,
                    "watches": [],
                },
                "decision_count": 0,
                "decisions": [],
            }
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
            agent_browser_session=None,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
            hard_margin_call_profit_floor=None,
            warn_only_default=True,
        ),
        capture_live=False,
    )

    assert snapshot["open_positions"][0]["event"] == "Tottenham vs Atlético Madrid"
    assert (
        snapshot["open_positions"][0]["event_status"]
        == "In 44 Minutes|UEFA Champions League"
    )
    assert (
        snapshot["open_positions"][0]["event_url"]
        == "https://smarkets.com/football/uefa-champions-league/2026/03/18/20-00/tottenham-hotspur-vs-atletico-de-madrid/44941563/"
    )
    assert snapshot["open_positions"][1]["event_status"] == "20 Mar 8:35 AM|Super Rugby"


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
        input=f"{load_dashboard_request}\n{json.dumps('RefreshCached')}\n",
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
        request_line=json.dumps("RefreshCached"),
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


def test_refresh_cached_request_uses_run_dir_state_without_live_recap(
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
        request="RefreshCached",
        config=resolved_config,
    )

    assert capture_count == 0
    assert first_response["snapshot"]["runtime"]["refresh_kind"] == "bootstrap"
    assert first_response["snapshot"]["runtime"]["source"] == "watcher-state"
    assert refresh_response["snapshot"]["runtime"]["refresh_kind"] == "cached"
    assert refresh_response["snapshot"]["runtime"]["source"] == "watcher-state"
    assert (
        refresh_response["snapshot"]["runtime"]["session"]["current_url"]
        == "https://smarkets.com/portfolio/"
    )


def test_refresh_live_request_recaptures_smarkets_state(
    tmp_path: Path,
    monkeypatch,
) -> None:
    run_dir = tmp_path / "smarkets-run"
    run_dir.mkdir()
    events_path = run_dir / "events.jsonl"
    events_path.write_text(
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

    capture_count = 0

    def fake_capture(config: WorkerConfig) -> None:
        nonlocal capture_count
        capture_count += 1
        events_path.write_text(
            json.dumps(
                {
                    "captured_at": "2026-03-11T11:06:00Z",
                    "source": "smarkets_exchange",
                    "kind": "positions_snapshot",
                    "page": "open_positions",
                    "url": "https://smarkets.com/open-positions",
                    "document_title": "Open positions",
                    "body_text": (
                        "Available balance £160.00 Exposure £19.00 Unrealized P/L £3.10 "
                        "Lazio vs Sassuolo "
                        "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£0.31 "
                        "(3.13%) Order filled Trade out Back 4.70"
                    ),
                    "interactive_snapshot": [],
                    "links": [],
                    "inputs": {},
                    "visible_actions": ["Trade out"],
                    "resource_hosts": ["smarkets.com"],
                    "local_storage_keys": [],
                    "screenshot_path": None,
                    "notes": ["manual-live-refresh"],
                },
            )
            + "\n",
        )

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

    refresh_response, _ = handle_worker_request(
        request="RefreshLive",
        config=config,
    )

    assert capture_count == 1
    assert refresh_response["snapshot"]["runtime"]["refresh_kind"] == "live_capture"
    assert refresh_response["snapshot"]["account_stats"]["available_balance"] == 160.0


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


def test_load_exchange_snapshot_skips_live_capture_when_watcher_state_is_fresh(
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
                "url": "https://smarkets.com/portfolio/?order-state=active",
                "document_title": "Smarkets Predictions",
                "body_text": (
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
                "updated_at": "2999-03-11T11:05:00Z",
                "interval_seconds": 5.0,
                "iteration": 9,
                "worker": {
                    "name": "bet-recorder",
                    "status": "ready",
                    "detail": "watcher stable",
                },
                "open_positions": [
                    {
                        "contract": "Draw",
                        "market": "Full-time result",
                        "event": "Lazio vs Sassuolo",
                        "event_status": "20 Mar 10:00 PM|Serie A",
                    }
                ],
                "session": {
                    "name": "helium-copy",
                    "current_url": "https://smarkets.com/portfolio/?order-state=active",
                    "document_title": "Smarkets Predictions",
                    "page_hint": "open_positions",
                    "open_positions_ready": True,
                    "validation_error": None,
                },
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

    def fail_capture(config: WorkerConfig) -> None:
        raise AssertionError("fresh watcher state should avoid live capture")

    monkeypatch.setattr(
        "bet_recorder.exchange_worker.capture_current_smarkets_open_positions",
        fail_capture,
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
        capture_live=True,
    )

    assert snapshot["runtime"]["source"] == "watcher-state"
    assert snapshot["worker"]["status"] == "ready"


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


def test_load_exchange_snapshot_prefers_newer_positions_payload_over_lagging_watcher_state(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "smarkets-run"
    run_dir.mkdir()
    (run_dir / "events.jsonl").write_text(
        json.dumps(
            {
                "captured_at": "2026-03-19T19:25:00Z",
                "source": "smarkets_exchange",
                "kind": "positions_snapshot",
                "page": "open_positions",
                "url": "https://smarkets.com/portfolio/?order-state=active",
                "document_title": "Smarkets Predictions",
                "body_text": (
                    "15:32 - Newbury\n"
                    "Tomorrow At 3:32 PM|Newbury\n"
                    "Contract\n"
                    "Price\n"
                    "Stake/\n"
                    "Liability\n"
                    "Return\n"
                    "Current Value\n"
                    "Status\n"
                    "Sell J J Moon\n"
                    "To win\n"
                    "7.2\n"
                    "£8.33\n"
                    "£51.64\n"
                    "£59.97\n"
                    "£7.57\n"
                    "-£0.76 (9.12%)\n"
                    "Order filled\n"
                    "Brumbies vs Chiefs\n"
                    "Tomorrow At 8:35 AM|Super Rugby\n"
                    "Contract\n"
                    "Price\n"
                    "Stake/\n"
                    "Liability\n"
                    "Return\n"
                    "Current Value\n"
                    "Status\n"
                    "Sell Brumbies\n"
                    "Winner (including overtime)\n"
                    "3.0\n"
                    "£10.40\n"
                    "£20.80\n"
                    "£31.20\n"
                    "£7.23\n"
                    "-£3.17 (30.48%)\n"
                    "Order filled\n"
                    "Trade out\n"
                    "Lazio vs Sassuolo\n"
                    "Tomorrow At 10:00 PM|Serie A\n"
                    "Contract\n"
                    "Price\n"
                    "Stake/\n"
                    "Liability\n"
                    "Return\n"
                    "Current Value\n"
                    "Status\n"
                    "Sell Draw\n"
                    "Full-time result\n"
                    "3.35\n"
                    "£9.91\n"
                    "£23.29\n"
                    "£33.20\n"
                    "£9.60\n"
                    "-£1.31 (3.13%)\n"
                    "Order filled\n"
                    "Trade out\n"
                ),
                "interactive_snapshot": [],
                "links": [],
                "inputs": {},
                "visible_actions": ["Trade out"],
                "resource_hosts": ["smarkets.com"],
                "local_storage_keys": [],
                "screenshot_path": None,
                "notes": ["exchange-worker-refresh", "scroll-capture"],
            },
        )
        + "\n",
    )
    (run_dir / "watcher-state.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-19T19:24:00Z",
                "interval_seconds": 5.0,
                "iteration": 42,
                "worker": {
                    "name": "bet-recorder",
                    "status": "ready",
                    "detail": "watcher stable",
                },
                "open_positions": [
                    {"contract": "J J Moon", "market": "To win"},
                    {"contract": "Brumbies", "market": "Winner (including overtime)"},
                ],
                "session": {
                    "name": "helium-copy",
                    "current_url": "https://smarkets.com/portfolio/?order-state=active",
                    "document_title": "Smarkets Predictions",
                    "page_hint": "open_positions",
                    "open_positions_ready": True,
                    "validation_error": None,
                },
                "decision_count": 2,
                "decisions": [{"contract": "J J Moon"}, {"contract": "Brumbies"}],
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
        capture_live=False,
    )

    assert len(snapshot["open_positions"]) == 3
    assert [position["contract"] for position in snapshot["open_positions"]] == [
        "J J Moon",
        "Brumbies",
        "Draw",
    ]
    assert snapshot["runtime"]["source"] == "watcher-state"


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


def test_handle_worker_request_line_preserves_selected_venue_after_invalid_select() -> None:
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
        request_line=json.dumps({"SelectVenue": {"venue": "betfair"}}),
        config=config,
        selected_venue="bet365",
    )

    assert next_config == config
    assert next_selected_venue == "bet365"
    assert response["request_error"] == "Unsupported venue for recorder worker: betfair"
    assert response["snapshot"]["selected_venue"] is None


def test_refresh_cached_reuses_cached_live_venue_snapshot(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "bet_recorder.exchange_worker.capture_current_live_venue_payload",
        lambda venue: (_ for _ in ()).throw(
            AssertionError("cached refresh should not capture live venue payload")
        ),
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
    cached_snapshot = {
        "worker": {
            "name": "bet-recorder",
            "status": "ready",
            "detail": "bet365 cache",
        },
        "venues": [],
        "selected_venue": "bet365",
        "events": [],
        "markets": [],
        "preflight": None,
        "status_line": "bet365 cache",
        "runtime": {
            "updated_at": "2026-03-11T11:05:00Z",
            "source": "bet365:my_bets",
            "refresh_kind": "live_capture",
            "decision_count": 0,
            "watcher_iteration": None,
            "stale": False,
        },
        "account_stats": None,
        "open_positions": [],
        "historical_positions": [],
        "ledger_pnl_summary": {},
        "other_open_bets": [],
        "decisions": [],
        "watch": None,
        "tracked_bets": [],
        "exit_policy": {},
        "exit_recommendations": [],
    }

    response, next_config, next_selected_venue = handle_worker_request_line(
        request_line=json.dumps("RefreshCached"),
        config=config,
        selected_venue="bet365",
        cached_snapshot=cached_snapshot,
    )

    assert next_config == config
    assert next_selected_venue == "bet365"
    assert response["snapshot"]["selected_venue"] == "bet365"
    assert response["snapshot"]["status_line"] == "bet365 cache"
    assert response["snapshot"]["runtime"]["refresh_kind"] == "cached"


def test_load_exchange_snapshot_handles_missing_live_tab_gracefully(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "bet_recorder.exchange_worker.list_debug_targets",
        lambda: (_ for _ in ()).throw(
            URLError(OSError(1, "Operation not permitted"))
        ),
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
        selected_venue="betfred",
    )

    assert snapshot["selected_venue"] == "betfred"
    assert snapshot["other_open_bets"] == []
    assert snapshot["worker"]["status"] == "error"
    assert "Operation not permitted" in snapshot["status_line"]


@pytest.mark.parametrize(
    ("venue", "url", "document_title", "body_text", "expected_label"),
    [
        (
            "betfred",
            "https://www.betfred.com/sport/my-bets",
            "Betfred My Bets",
            "My Bets\nOpen\nCash Out\nEverton v Liverpool\nLiverpool\n7/4\nMatch Result\nStake\n£12.00",
            "Liverpool",
        ),
        (
            "coral",
            "https://sports.coral.co.uk/my-bets",
            "Coral My Bets",
            "Open Bets\nCheltenham 15:20\nDesert Hero\n3.50\nWin Market\nStake\n£8.00\nCash Out",
            "Desert Hero",
        ),
        (
            "ladbrokes",
            "https://sports.ladbrokes.com/my-bets",
            "Ladbrokes My Bets",
            "My Bets\nArsenal v Everton\nArsenal\n2.40\nMatch Odds\n£10.00\nCash Out",
            "Arsenal",
        ),
        (
            "kwik",
            "https://sports.kwiff.com/my-bets",
            "Kwik My Bets",
            "Open Bets\nEngland v France\nFrance\n2.80\nMatch Odds\n£5.00",
            "France",
        ),
        (
            "bet600",
            "https://www.bet600.co.uk/my-bets",
            "Bet600 My Bets",
            "My Bets\nOpen\nBarcelona v Real Madrid\nBoth Teams To Score\n1.95\nSpecials\n£7.50",
            "Both Teams To Score",
        ),
    ],
)
def test_load_exchange_snapshot_supports_added_live_bookmaker_payloads(
    monkeypatch,
    venue: str,
    url: str,
    document_title: str,
    body_text: str,
    expected_label: str,
) -> None:
    monkeypatch.setattr(
        "bet_recorder.exchange_worker.capture_current_live_venue_payload",
        lambda requested_venue: {
            "page": "my_bets",
            "url": url,
            "document_title": document_title,
            "body_text": body_text,
            "inputs": {},
            "visible_actions": ["My Bets", "Cash Out"],
            "captured_at": "2026-03-19T08:15:00Z",
        },
    )
    monkeypatch.setattr(
        "bet_recorder.exchange_worker.list_debug_targets",
        lambda: [
            DebugTarget(
                target_id=f"{venue}-live",
                target_type="page",
                title=document_title,
                url=url,
                websocket_debugger_url=f"ws://example.invalid/{venue}",
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
        selected_venue=venue,
    )

    assert snapshot["selected_venue"] == venue
    assert snapshot["worker"]["status"] == "ready"
    assert snapshot["other_open_bets"][0]["label"] == expected_label


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
