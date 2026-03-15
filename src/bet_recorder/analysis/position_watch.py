from __future__ import annotations

from collections import OrderedDict

from bet_recorder.analysis.trade_out import exit_odds_for_target_profit


def build_smarkets_watch_plan(
  *,
  positions: list[dict],
  commission_rate: float,
  target_profit: float,
  stop_loss: float,
) -> dict:
  grouped_positions = _group_positions(positions)
  watches = [
    _build_watch(
      contract=contract,
      market=market,
      grouped_positions=group,
      commission_rate=commission_rate,
      target_profit=target_profit,
      stop_loss=stop_loss,
    )
    for (market, contract), group in grouped_positions.items()
  ]
  return {
    "position_count": len(positions),
    "watch_count": len(watches),
    "commission_rate": commission_rate,
    "target_profit": target_profit,
    "stop_loss": stop_loss,
    "watches": watches,
  }


def _group_positions(positions: list[dict]) -> OrderedDict[tuple[str, str], list[dict]]:
  grouped: OrderedDict[tuple[str, str], list[dict]] = OrderedDict()
  for position in positions:
    key = (position["market"], position["contract"])
    grouped.setdefault(key, []).append(position)
  return grouped


def _build_watch(
  *,
  contract: str,
  market: str,
  grouped_positions: list[dict],
  commission_rate: float,
  target_profit: float,
  stop_loss: float,
) -> dict:
  total_stake = sum(float(position["stake"]) for position in grouped_positions)
  total_liability = sum(float(position["liability"]) for position in grouped_positions)
  weighted_entry_odds = sum(
    float(position["stake"]) * float(position["price"])
    for position in grouped_positions
  ) / total_stake
  profit_take_back_odds = exit_odds_for_target_profit(
    entry_lay_odds=weighted_entry_odds,
    lay_stake=total_stake,
    commission_rate=commission_rate,
    target_profit=target_profit,
  )
  stop_loss_back_odds = exit_odds_for_target_profit(
    entry_lay_odds=weighted_entry_odds,
    lay_stake=total_stake,
    commission_rate=commission_rate,
    target_profit=-stop_loss,
  )
  current_back_odds = _weighted_current_back_odds(grouped_positions)

  return {
    "contract": contract,
    "market": market,
    "position_count": len(grouped_positions),
    "can_trade_out": any(bool(position.get("can_trade_out")) for position in grouped_positions),
    "total_stake": total_stake,
    "total_liability": total_liability,
    "current_pnl_amount": sum(float(position["pnl_amount"]) for position in grouped_positions),
    "current_back_odds": current_back_odds,
    "average_entry_lay_odds": weighted_entry_odds,
    "entry_implied_probability": 1 / weighted_entry_odds,
    "profit_take_back_odds": profit_take_back_odds,
    "profit_take_implied_probability": 1 / profit_take_back_odds,
    "stop_loss_back_odds": stop_loss_back_odds,
    "stop_loss_implied_probability": 1 / stop_loss_back_odds,
  }


def _weighted_current_back_odds(grouped_positions: list[dict]) -> float | None:
  positions_with_back_odds = [
    position
    for position in grouped_positions
    if position.get("current_back_odds") is not None
  ]
  if not positions_with_back_odds:
    return None

  total_stake = sum(float(position["stake"]) for position in positions_with_back_odds)
  return sum(
    float(position["stake"]) * float(position["current_back_odds"])
    for position in positions_with_back_odds
  ) / total_stake
