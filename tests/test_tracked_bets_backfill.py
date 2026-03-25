from pathlib import Path
import json
import sys

from typer.testing import CliRunner

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.cli import app  # noqa: E402
from bet_recorder.ledger.loader import load_tracked_bets  # noqa: E402
from bet_recorder.ledger.tracked_bets_backfill import build_backfilled_tracked_bets  # noqa: E402


def test_build_backfilled_tracked_bets_matches_sportsbook_history_to_smarkets_lay() -> None:
    payload = {
        "ledger_entries": [
            {
                "entry_id": "bet10:won",
                "occurred_at": "2026-03-22T16:10:26Z",
                "platform": "bet10",
                "activity_type": "bet_settled",
                "status": "won",
                "platform_kind": "sportsbook",
                "event": "Arsenal v Everton",
                "market": "Full-time result",
                "selection": "Draw",
                "bet_type": "single",
                "market_family": "match_odds",
                "funding_kind": "free_bet",
                "sport_name": "Premier League",
                "stake_gbp": 10.0,
                "odds_decimal": 4.2,
                "payout_gbp": 42.0,
                "realised_pnl_gbp": 32.0,
                "source_file": "bet10.txt",
                "description": "Free Bet SNR",
            },
            {
                "entry_id": "smarkets:placed",
                "occurred_at": "2026-03-22T15:05:00Z",
                "platform": "smarkets",
                "activity_type": "bet_placed",
                "status": "placed",
                "platform_kind": "exchange",
                "exchange": "smarkets",
                "event": "Arsenal vs Everton",
                "market": "Match Odds",
                "selection": "Draw",
                "side": "lay",
                "market_family": "match_odds",
                "stake_gbp": 9.91,
                "odds_decimal": 3.35,
                "exposure_gbp": -23.29,
                "source_file": "smarkets.csv",
                "description": "Bet Placed · Against Draw",
            },
        ]
    }

    backfill_payload = build_backfilled_tracked_bets(payload)

    assert backfill_payload["summary"]["matched_tracked_bet_count"] == 1
    assert backfill_payload["unmatched_entries"] == []
    tracked_bet = backfill_payload["tracked_bets"][0]
    assert tracked_bet["platform"] == "bet10"
    assert tracked_bet["exchange"] == "smarkets"
    assert tracked_bet["funding_kind"] == "free_bet"
    assert tracked_bet["selection"] == "Draw"
    assert tracked_bet["lay_price"] == 3.35
    assert tracked_bet["back_price"] == 4.2
    assert tracked_bet["realised_pnl_gbp"] == 8.71
    assert tracked_bet["legs"][0]["venue"] == "smarkets"
    assert tracked_bet["legs"][0]["status"] == "lost"
    assert tracked_bet["legs"][1]["venue"] == "bet10"
    assert tracked_bet["legs"][1]["status"] == "won"


def test_build_backfilled_tracked_bets_reports_unmatched_sportsbook_entries() -> None:
    payload = {
        "ledger_entries": [
            {
                "entry_id": "bet10:lost",
                "occurred_at": "2026-03-22T16:10:26Z",
                "platform": "bet10",
                "activity_type": "bet_settled",
                "status": "lost",
                "platform_kind": "sportsbook",
                "event": "Arsenal v Everton",
                "market": "Full-time result",
                "selection": "Draw",
                "bet_type": "single",
                "market_family": "match_odds",
                "stake_gbp": 10.0,
                "odds_decimal": 4.2,
                "payout_gbp": 0.0,
                "realised_pnl_gbp": -10.0,
                "source_file": "bet10.txt",
                "description": "Lost",
            }
        ]
    }

    backfill_payload = build_backfilled_tracked_bets(payload)

    assert backfill_payload["tracked_bets"] == []
    assert backfill_payload["summary"]["unmatched_sportsbook_entry_count"] == 1
    assert backfill_payload["unmatched_entries"][0]["entry_id"] == "bet10:lost"
    assert backfill_payload["unmatched_entries"][0]["reason"] == "no_matching_smarkets_leg"


def test_backfill_tracked_bets_command_writes_loader_compatible_payload(
    tmp_path: Path,
) -> None:
    statement_history_path = tmp_path / "statement-history.json"
    statement_history_path.write_text(
        json.dumps(
            {
                "ledger_entries": [
                    {
                        "entry_id": "bet10:won",
                        "occurred_at": "2026-03-22T16:10:26Z",
                        "platform": "bet10",
                        "activity_type": "bet_settled",
                        "status": "won",
                        "platform_kind": "sportsbook",
                        "event": "Arsenal v Everton",
                        "market": "Full-time result",
                        "selection": "Draw",
                        "bet_type": "single",
                        "market_family": "match_odds",
                        "funding_kind": "free_bet",
                        "sport_name": "Premier League",
                        "stake_gbp": 10.0,
                        "odds_decimal": 4.2,
                        "payout_gbp": 42.0,
                        "realised_pnl_gbp": 32.0,
                        "source_file": "bet10.txt",
                        "description": "Free Bet SNR",
                    },
                    {
                        "entry_id": "smarkets:placed",
                        "occurred_at": "2026-03-22T15:05:00Z",
                        "platform": "smarkets",
                        "activity_type": "bet_placed",
                        "status": "placed",
                        "platform_kind": "exchange",
                        "exchange": "smarkets",
                        "event": "Arsenal vs Everton",
                        "market": "Match Odds",
                        "selection": "Draw",
                        "side": "lay",
                        "market_family": "match_odds",
                        "stake_gbp": 9.91,
                        "odds_decimal": 3.35,
                        "exposure_gbp": -23.29,
                        "source_file": "smarkets.csv",
                        "description": "Bet Placed · Against Draw",
                    },
                ]
            }
        )
        + "\n"
    )
    output_path = tmp_path / "tracked-bets.json"

    result = CliRunner().invoke(
        app,
        [
            "backfill-tracked-bets",
            "--statement-history-path",
            str(statement_history_path),
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert output_path.exists()
    tracked_bets = load_tracked_bets(output_path)
    assert len(tracked_bets) == 1
    assert tracked_bets[0]["funding_kind"] == "free_bet"
    assert tracked_bets[0]["expected_ev"]["status"] == "calculated"
