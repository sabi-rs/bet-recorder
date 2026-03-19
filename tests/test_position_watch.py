from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.analysis.position_watch import build_smarkets_watch_plan  # noqa: E402


def test_build_smarkets_watch_plan_groups_duplicate_lays_and_solves_thresholds() -> None:
  watch_plan = build_smarkets_watch_plan(
    positions=[
      {
        "contract": "1 - 1",
        "market": "Correct score",
        "price": 7.2,
        "stake": 2.55,
        "liability": 15.81,
        "return_amount": 18.36,
        "current_value": 2.46,
        "pnl_amount": -0.09,
        "pnl_percent": 3.53,
        "status": "Order filled",
        "current_back_odds": 10.87,
        "can_trade_out": True,
      },
      {
        "contract": "1 - 1",
        "market": "Correct score",
        "price": 7.2,
        "stake": 0.41,
        "liability": 2.53,
        "return_amount": 2.94,
        "current_value": 0.32,
        "pnl_amount": -0.09,
        "pnl_percent": 22.05,
        "status": "Order filled",
        "current_back_odds": 10.87,
        "can_trade_out": True,
      },
      {
        "contract": "Draw",
        "market": "Full-time result",
        "price": 3.35,
        "stake": 9.91,
        "liability": 23.29,
        "return_amount": 33.20,
        "current_value": 9.60,
        "pnl_amount": -0.31,
        "pnl_percent": 3.13,
        "status": "Order filled",
        "current_back_odds": 2.80,
        "can_trade_out": True,
      },
    ],
    commission_rate=0.0,
    target_profit=1.0,
    stop_loss=1.0,
  )

  assert watch_plan["position_count"] == 3
  assert watch_plan["watch_count"] == 2

  one_one = watch_plan["watches"][0]
  assert one_one["contract"] == "1 - 1"
  assert one_one["market"] == "Correct score"
  assert one_one["position_count"] == 2
  assert round(one_one["total_stake"], 2) == 2.96
  assert round(one_one["total_liability"], 2) == 18.34
  assert round(one_one["current_back_odds"], 2) == 10.87
  assert round(one_one["profit_take_back_odds"], 2) == 10.87
  assert round(one_one["stop_loss_back_odds"], 2) == 5.38

  draw = watch_plan["watches"][1]
  assert draw["contract"] == "Draw"
  assert draw["market"] == "Full-time result"
  assert draw["position_count"] == 1
  assert round(draw["current_back_odds"], 2) == 2.80
  assert round(draw["profit_take_back_odds"], 2) == 3.73
  assert round(draw["stop_loss_back_odds"], 2) == 3.04


def test_build_smarkets_watch_plan_ignores_buy_positions_for_exit_thresholds() -> None:
  watch_plan = build_smarkets_watch_plan(
    positions=[
      {
        "contract": "0 - 0",
        "market": "Correct score",
        "side": "buy",
        "price": 14.0,
        "stake": 2.0,
        "liability": 2.0,
        "return_amount": 28.01,
        "current_value": 1.93,
        "pnl_amount": -0.07,
        "pnl_percent": 3.5,
        "status": "Order filled",
        "current_back_odds": None,
        "can_trade_out": False,
      },
      {
        "contract": "Draw",
        "market": "Full-time result",
        "side": "sell",
        "price": 4.2,
        "stake": 8.0,
        "liability": 25.6,
        "return_amount": 33.6,
        "current_value": 7.39,
        "pnl_amount": -0.61,
        "pnl_percent": 7.63,
        "status": "Order filled",
        "current_back_odds": None,
        "can_trade_out": False,
      },
    ],
    commission_rate=0.0,
    target_profit=1.0,
    stop_loss=1.0,
  )

  assert watch_plan["position_count"] == 2
  assert watch_plan["watch_count"] == 1
  assert watch_plan["watches"][0]["contract"] == "Draw"
