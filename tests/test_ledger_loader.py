from pathlib import Path
import sys
import json

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.ledger.loader import load_tracked_bets  # noqa: E402


def test_load_tracked_bets_reads_legacy_companion_leg_file(tmp_path: Path) -> None:
    companion_legs_path = tmp_path / "companion-legs.json"
    companion_legs_path.write_text(
        json.dumps(
            {
                "tracked_bets": [
                    {
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
                                "odds": 3.35,
                                "stake": 9.91,
                                "status": "open",
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

    tracked_bets = load_tracked_bets(companion_legs_path)

    assert len(tracked_bets) == 1
    tracked_bet = tracked_bets[0]
    assert tracked_bet["bet_id"] == "bet-001"
    assert tracked_bet["platform"] == "bet365"
    assert tracked_bet["exchange"] == "smarkets"
    assert tracked_bet["bet_type"] == "single"
    assert tracked_bet["market_family"] == "match_odds"
    assert tracked_bet["back_price"] == 2.12
    assert tracked_bet["lay_price"] == 3.35
    assert tracked_bet["legs"][1]["venue"] == "bet365"
    assert tracked_bet["expected_ev"]["source"] == "local_formula"


def test_load_tracked_bets_keeps_rich_fields_and_calculates_locked_profit_ev(
    tmp_path: Path,
) -> None:
    companion_legs_path = tmp_path / "companion-legs.json"
    companion_legs_path.write_text(
        json.dumps(
            {
                "tracked_bets": [
                    {
                        "bet_id": "bet-locked",
                        "group_id": "group-locked",
                        "placed_at": "2026-03-13T10:30:00Z",
                        "platform": "betmgm",
                        "exchange": "smarkets",
                        "sport_key": "soccer_epl",
                        "sport_name": "Premier League",
                        "event": "Arsenal v Everton",
                        "market": "Full-time result",
                        "bet_type": "single",
                        "selection": "Draw",
                        "status": "open",
                        "stake_gbp": 10.0,
                        "odds_reference": {"fair_price": 2.5},
                        "activities": [
                            {
                                "occurred_at": "2026-03-13T10:30:00Z",
                                "activity_type": "placed",
                                "amount_gbp": -10.0,
                                "source_file": "betmgm.csv",
                            }
                        ],
                        "legs": [
                            {
                                "venue": "betmgm",
                                "outcome": "Draw",
                                "side": "back",
                                "odds": 2.5,
                                "stake": 10.0,
                                "status": "matched",
                            },
                            {
                                "venue": "smarkets",
                                "outcome": "Draw",
                                "side": "lay",
                                "odds": 2.0,
                                "stake": 12.5,
                                "status": "open",
                                "commission_rate": 0.0,
                            },
                        ],
                    }
                ]
            }
        )
    )

    tracked_bet = load_tracked_bets(companion_legs_path)[0]

    assert tracked_bet["platform"] == "betmgm"
    assert tracked_bet["sport_key"] == "soccer_epl"
    assert tracked_bet["expected_ev"]["method"] == "matched_pair_locked_profit"
    assert tracked_bet["expected_ev"]["gbp"] == 2.5
    assert tracked_bet["expected_ev"]["pct"] == 0.25
    assert tracked_bet["activities"][0]["activity_type"] == "placed"
