from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.ledger.expected_value import (  # noqa: E402
    calculate_expected_value,
    calculate_realised_value,
)


def test_expected_value_uses_fair_price_for_back_bet() -> None:
    tracked_bet = {
        "stake_gbp": 10.0,
        "back_price": 2.4,
        "legs": [
            {
                "venue": "bet365",
                "outcome": "Draw",
                "side": "back",
                "odds": 2.4,
                "stake": 10.0,
                "status": "matched",
            }
        ],
        "odds_reference": {"fair_price": 2.0},
    }

    expected_ev = calculate_expected_value(tracked_bet)

    assert expected_ev["status"] == "calculated"
    assert round(expected_ev["gbp"], 2) == 2.0
    assert round(expected_ev["pct"], 2) == 0.2
    assert expected_ev["method"] == "fair_price"


def test_realised_value_uses_realised_minus_expected_when_available() -> None:
    tracked_bet = {
        "stake_gbp": 10.0,
        "realised_pnl_gbp": 5.0,
        "realised_ev": {},
    }

    realised_ev = calculate_realised_value(tracked_bet, {"gbp": 2.0})

    assert realised_ev["status"] == "calculated"
    assert realised_ev["gbp"] == 3.0
    assert realised_ev["pct"] == 0.3
