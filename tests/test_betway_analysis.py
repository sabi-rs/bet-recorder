from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.analysis.betway_uk import analyze_betway_page  # noqa: E402


def test_analyze_betway_betslip_extracts_core_builder_fields() -> None:
  analysis = analyze_betway_page(
    page="betslip",
    body_text=(
      "4 Bet Slip Odds 7/1 Almeria win Under 2.5 Goals Both Teams To Score No "
      "Almeria Team Goals Over 1.5 Free Bet Stake 10 Place Bet"
    ),
    inputs={"stake": "10"},
    visible_actions=["Place Bet"],
  )

  assert analysis["page"] == "betslip"
  assert analysis["selection_count"] == 4
  assert analysis["odds_fractional"] == "7/1"
  assert analysis["odds_decimal"] == 8.0
  assert analysis["stake"] == 10.0
  assert analysis["free_bet"] is True
  assert analysis["can_place_bet"] is True


def test_analyze_betway_my_bets_detects_login_required_state() -> None:
  analysis = analyze_betway_page(
    page="my_bets",
    body_text=(
      "My Bets is empty Please login to view your bets. "
      "Login Help Betting Rules Cash Out"
    ),
    inputs={},
    visible_actions=["Login", "Help", "Cash Out"],
  )

  assert analysis["page"] == "my_bets"
  assert analysis["status"] == "login_required"
  assert analysis["open_bet_count"] == 0
  assert analysis["supports_cash_out"] is True
