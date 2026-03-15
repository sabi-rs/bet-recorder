from pathlib import Path
import sys
import json

from typer.testing import CliRunner

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.cli import app


def test_exchange_worker_stdio_returns_transport_snapshot(tmp_path: Path) -> None:
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

    result = runner.invoke(app, ["exchange-worker-stdio"], input=load_dashboard_request)

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["snapshot"]["selected_venue"] == "smarkets"
    assert payload["snapshot"]["account_stats"]["available_balance"] == 120.45
    assert payload["snapshot"]["open_positions"][0]["contract"] == "1 - 1"
    assert payload["snapshot"]["other_open_bets"][0]["label"] == "Arsenal"
    assert payload["snapshot"]["runtime"]["source"] == "positions_snapshot"
    assert payload["snapshot"]["watch"]["watch_count"] == 2
    assert "bet-recorder" in payload["snapshot"]["status_line"]


def fixture_text(name: str) -> str:
    return (Path(__file__).resolve().parent / "fixtures" / "worker" / name).read_text()
