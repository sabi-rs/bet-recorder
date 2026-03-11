from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.analysis.trade_out import (  # noqa: E402
  exit_odds_for_target_profit,
  lay_position_trade_out,
)


def test_lay_position_trade_out_calculates_full_hedge_and_locked_profit() -> None:
  analysis = lay_position_trade_out(
    entry_lay_odds=3.35,
    lay_stake=9.91,
    current_back_odds=4.8,
    commission_rate=0.0,
  )

  assert round(analysis["hedge_back_stake"], 2) == 6.92
  assert round(analysis["locked_profit"], 2) == 2.99
  assert round(analysis["entry_implied_probability"], 4) == 0.2985
  assert round(analysis["current_implied_probability"], 4) == 0.2083


def test_exit_odds_for_target_profit_solves_back_odds() -> None:
  target_odds = exit_odds_for_target_profit(
    entry_lay_odds=7.2,
    lay_stake=2.96,
    commission_rate=0.0,
    target_profit=1.0,
  )

  assert round(target_odds, 2) == 10.87
