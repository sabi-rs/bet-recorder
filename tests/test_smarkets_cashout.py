from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.actions.smarkets_cashout import handle_cash_out_tracked_bet  # noqa: E402
from bet_recorder.exchange_worker import handle_worker_request  # noqa: E402


def test_cash_out_tracked_bet_requires_actionable_smarkets_session() -> None:
    snapshot = sample_snapshot()
    snapshot["runtime"] = {
        "session": {
            "current_url": "about:blank",
            "document_title": "Blank page",
            "open_positions_ready": False,
        },
    }

    with pytest.raises(
        ValueError, match="not on an actionable Smarkets open positions page"
    ):
        handle_cash_out_tracked_bet(snapshot=snapshot, bet_id="bet-001")


def test_cash_out_tracked_bet_refuses_until_execution_contract_is_captured() -> None:
    snapshot = sample_snapshot()

    updated = handle_cash_out_tracked_bet(snapshot=snapshot, bet_id="bet-001")

    assert updated["worker"]["status"] == "error"
    assert "execution contract is not implemented yet" in updated["worker"]["detail"]
    assert "execution contract is not implemented yet" in updated["status_line"]


def test_worker_request_rejects_generic_bet_placement_request() -> None:
    with pytest.raises(ValueError, match="Unsupported worker request"):
        handle_worker_request(
            request={"PlaceBet": {"bet_id": "bet-001"}},
            config=None,
        )


def sample_snapshot() -> dict:
    return {
        "worker": {
            "name": "bet-recorder",
            "status": "ready",
            "detail": "snapshot ready",
        },
        "status_line": "snapshot ready",
        "runtime": {
            "session": {
                "current_url": "https://smarkets.com/portfolio/",
                "document_title": "Open positions",
                "open_positions_ready": True,
            },
        },
        "tracked_bets": [
            {
                "bet_id": "bet-001",
                "selection": "Draw",
            },
        ],
        "exit_recommendations": [
            {
                "bet_id": "bet-001",
                "action": "cash_out",
                "reason": "hard_margin_call",
                "cash_out_venue": "smarkets",
            },
        ],
    }
