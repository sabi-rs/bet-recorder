from __future__ import annotations


def lay_position_trade_out(
  *,
  entry_lay_odds: float,
  lay_stake: float,
  current_back_odds: float,
  commission_rate: float,
) -> dict:
  effective_commission = _normalize_commission_rate(commission_rate)
  hedge_back_stake = (lay_stake * (entry_lay_odds - effective_commission)) / current_back_odds
  locked_profit = (lay_stake * (1 - effective_commission)) - hedge_back_stake
  return {
    "entry_lay_odds": entry_lay_odds,
    "lay_stake": lay_stake,
    "current_back_odds": current_back_odds,
    "commission_rate": effective_commission,
    "hedge_back_stake": hedge_back_stake,
    "locked_profit": locked_profit,
    "entry_implied_probability": 1 / entry_lay_odds,
    "current_implied_probability": 1 / current_back_odds,
  }


def exit_odds_for_target_profit(
  *,
  entry_lay_odds: float,
  lay_stake: float,
  commission_rate: float,
  target_profit: float,
) -> float:
  effective_commission = _normalize_commission_rate(commission_rate)
  denominator = (lay_stake * (1 - effective_commission)) - target_profit
  if denominator <= 0:
    raise ValueError("Target profit is not achievable for this lay position.")
  return (lay_stake * (entry_lay_odds - effective_commission)) / denominator


def _normalize_commission_rate(value: float) -> float:
  if value > 1:
    return value / 100
  return value
