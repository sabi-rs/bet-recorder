from __future__ import annotations

from bet_recorder.analysis.trade_out import lay_position_trade_out


def build_exit_recommendations(
    *,
    tracked_bets: list[dict],
    open_positions: list[dict],
    commission_rate: float,
    target_profit: float,
    stop_loss: float,
    hard_margin_call_profit_floor: float | None = None,
    warn_only_default: bool = True,
) -> list[dict]:
    return [
        _build_exit_recommendation(
            tracked_bet=tracked_bet,
            open_positions=open_positions,
            commission_rate=commission_rate,
            target_profit=target_profit,
            stop_loss=stop_loss,
            hard_margin_call_profit_floor=hard_margin_call_profit_floor,
            warn_only_default=warn_only_default,
        )
        for tracked_bet in tracked_bets
    ]


def _build_exit_recommendation(
    *,
    tracked_bet: dict,
    open_positions: list[dict],
    commission_rate: float,
    target_profit: float,
    stop_loss: float,
    hard_margin_call_profit_floor: float | None,
    warn_only_default: bool,
) -> dict:
    smarkets_leg = next(
        (
            leg
            for leg in tracked_bet.get("legs", [])
            if str(leg.get("venue", "")).lower() == "smarkets"
        ),
        None,
    )
    if smarkets_leg is None:
        return _recommendation(
            tracked_bet=tracked_bet,
            action="hold",
            reason="missing_smarkets_leg",
            worst_case_pnl=0.0,
            cash_out_venue=None,
        )

    if str(smarkets_leg.get("side", "")).lower() != "lay":
        return _recommendation(
            tracked_bet=tracked_bet,
            action="hold",
            reason="unsupported_smarkets_side",
            worst_case_pnl=0.0,
            cash_out_venue=None,
        )

    selection = str(tracked_bet.get("selection", smarkets_leg.get("outcome", "")))
    if any(
        str(leg.get("venue", "")).lower() != "smarkets"
        and str(leg.get("outcome", "")) != selection
        for leg in tracked_bet.get("legs", [])
    ):
        return _recommendation(
            tracked_bet=tracked_bet,
            action="hold",
            reason="unsupported_outcome_mapping",
            worst_case_pnl=0.0,
            cash_out_venue="smarkets",
        )

    live_position = _find_live_position(
        tracked_bet=tracked_bet, open_positions=open_positions
    )
    if live_position is None:
        return _recommendation(
            tracked_bet=tracked_bet,
            action="hold",
            reason="missing_live_position",
            worst_case_pnl=0.0,
            cash_out_venue="smarkets",
        )

    current_back_odds = live_position.get("current_back_odds")
    if not bool(live_position.get("can_trade_out")) or current_back_odds is None:
        return _recommendation(
            tracked_bet=tracked_bet,
            action="hold",
            reason="cash_out_unavailable",
            worst_case_pnl=0.0,
            cash_out_venue="smarkets",
        )

    trade_out = lay_position_trade_out(
        entry_lay_odds=float(smarkets_leg["odds"]),
        lay_stake=float(smarkets_leg["stake"]),
        current_back_odds=float(current_back_odds),
        commission_rate=commission_rate,
    )
    locked_profit = float(trade_out["locked_profit"])

    selection_wins_pnl = locked_profit + sum(
        _settled_leg_pnl(leg=leg, selection_wins=True)
        for leg in tracked_bet.get("legs", [])
        if str(leg.get("venue", "")).lower() != "smarkets"
    )
    selection_loses_pnl = locked_profit + sum(
        _settled_leg_pnl(leg=leg, selection_wins=False)
        for leg in tracked_bet.get("legs", [])
        if str(leg.get("venue", "")).lower() != "smarkets"
    )
    worst_case_pnl = min(selection_wins_pnl, selection_loses_pnl)

    if (
        hard_margin_call_profit_floor is not None
        and worst_case_pnl >= hard_margin_call_profit_floor
    ):
        return _recommendation(
            tracked_bet=tracked_bet,
            action="cash_out",
            reason="hard_margin_call",
            worst_case_pnl=worst_case_pnl,
            cash_out_venue="smarkets",
        )

    if worst_case_pnl >= target_profit:
        return _recommendation(
            tracked_bet=tracked_bet,
            action="warn" if warn_only_default else "cash_out",
            reason="target_profit",
            worst_case_pnl=worst_case_pnl,
            cash_out_venue="smarkets",
        )

    if worst_case_pnl <= -stop_loss:
        return _recommendation(
            tracked_bet=tracked_bet,
            action="warn" if warn_only_default else "cash_out",
            reason="stop_loss",
            worst_case_pnl=worst_case_pnl,
            cash_out_venue="smarkets",
        )

    return _recommendation(
        tracked_bet=tracked_bet,
        action="hold",
        reason="within_thresholds",
        worst_case_pnl=worst_case_pnl,
        cash_out_venue="smarkets",
    )


def _find_live_position(
    *, tracked_bet: dict, open_positions: list[dict]
) -> dict | None:
    selection = str(tracked_bet.get("selection", ""))
    market = str(tracked_bet.get("market", ""))
    return next(
        (
            position
            for position in open_positions
            if str(position.get("contract", "")) == selection
            and str(position.get("market", "")) == market
        ),
        None,
    )


def _settled_leg_pnl(*, leg: dict, selection_wins: bool) -> float:
    stake = float(leg["stake"])
    odds = float(leg["odds"])
    side = str(leg["side"]).lower()
    if side == "back":
        return stake * (odds - 1.0) if selection_wins else -stake
    if side == "lay":
        return -(stake * (odds - 1.0)) if selection_wins else stake
    raise ValueError(f"Unsupported leg side: {leg['side']}")


def _recommendation(
    *,
    tracked_bet: dict,
    action: str,
    reason: str,
    worst_case_pnl: float,
    cash_out_venue: str | None,
) -> dict:
    return {
        "bet_id": str(tracked_bet.get("bet_id", "")),
        "action": action,
        "reason": reason,
        "worst_case_pnl": worst_case_pnl,
        "cash_out_venue": cash_out_venue,
    }
