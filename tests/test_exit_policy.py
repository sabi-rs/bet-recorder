from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.ledger.policy import build_exit_recommendations  # noqa: E402


def test_build_exit_recommendations_returns_hold_within_thresholds() -> None:
    recommendations = build_exit_recommendations(
        tracked_bets=[sample_tracked_bet()],
        open_positions=[sample_open_position(current_back_odds=5.0)],
        commission_rate=0.0,
        target_profit=3.0,
        stop_loss=5.0,
    )

    assert recommendations[0]["action"] == "hold"
    assert recommendations[0]["reason"] == "within_thresholds"


def test_build_exit_recommendations_warns_for_target_profit_by_default() -> None:
    recommendations = build_exit_recommendations(
        tracked_bets=[sample_tracked_bet()],
        open_positions=[sample_open_position(current_back_odds=10.0)],
        commission_rate=0.0,
        target_profit=3.0,
        stop_loss=5.0,
    )

    assert recommendations[0]["action"] == "warn"
    assert recommendations[0]["reason"] == "target_profit"
    assert recommendations[0]["worst_case_pnl"] > 3.0


def test_build_exit_recommendations_cash_outs_target_profit_when_warn_only_disabled() -> (
    None
):
    recommendations = build_exit_recommendations(
        tracked_bets=[sample_tracked_bet()],
        open_positions=[sample_open_position(current_back_odds=10.0)],
        commission_rate=0.0,
        target_profit=3.0,
        stop_loss=5.0,
        warn_only_default=False,
    )

    assert recommendations[0]["action"] == "cash_out"
    assert recommendations[0]["reason"] == "target_profit"


def test_build_exit_recommendations_cash_outs_stop_loss_when_warn_only_disabled() -> (
    None
):
    recommendations = build_exit_recommendations(
        tracked_bets=[sample_tracked_bet()],
        open_positions=[sample_open_position(current_back_odds=2.5)],
        commission_rate=0.0,
        target_profit=3.0,
        stop_loss=5.0,
        warn_only_default=False,
    )

    assert recommendations[0]["action"] == "cash_out"
    assert recommendations[0]["reason"] == "stop_loss"
    assert recommendations[0]["worst_case_pnl"] < -5.0


def test_build_exit_recommendations_cash_outs_hard_margin_call() -> None:
    recommendations = build_exit_recommendations(
        tracked_bets=[sample_tracked_bet()],
        open_positions=[sample_open_position(current_back_odds=10.0)],
        commission_rate=0.0,
        target_profit=10.0,
        stop_loss=5.0,
        hard_margin_call_profit_floor=3.5,
    )

    assert recommendations[0]["action"] == "cash_out"
    assert recommendations[0]["reason"] == "hard_margin_call"


def sample_tracked_bet() -> dict:
    return {
        "bet_id": "bet-001",
        "group_id": "group-arsenal-everton",
        "event": "Arsenal v Everton",
        "market": "Full-time result",
        "selection": "Draw",
        "status": "open",
        "legs": [
            {
                "venue": "smarkets",
                "outcome": "Draw",
                "side": "lay",
                "odds": 4.0,
                "stake": 10.0,
                "status": "open",
            },
            {
                "venue": "bet365",
                "outcome": "Draw",
                "side": "back",
                "odds": 2.0,
                "stake": 1.5,
                "status": "matched",
            },
        ],
    }


def sample_open_position(*, current_back_odds: float) -> dict:
    return {
        "contract": "Draw",
        "market": "Full-time result",
        "price": 4.0,
        "stake": 10.0,
        "liability": 30.0,
        "current_value": 10.0,
        "pnl_amount": 0.0,
        "current_back_odds": current_back_odds,
        "can_trade_out": True,
    }
