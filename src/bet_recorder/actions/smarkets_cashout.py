from __future__ import annotations


def handle_cash_out_tracked_bet(*, snapshot: dict, bet_id: str) -> dict:
    tracked_bet = next(
        (
            tracked_bet
            for tracked_bet in snapshot.get("tracked_bets", [])
            if tracked_bet.get("bet_id") == bet_id
        ),
        None,
    )
    if tracked_bet is None:
        raise ValueError(f"Tracked bet not found: {bet_id}")

    recommendation = next(
        (
            recommendation
            for recommendation in snapshot.get("exit_recommendations", [])
            if recommendation.get("bet_id") == bet_id
        ),
        None,
    )
    if recommendation is None:
        raise ValueError(
            f"No exit recommendation is available for tracked bet: {bet_id}"
        )
    if recommendation.get("cash_out_venue") != "smarkets":
        raise ValueError(f"Tracked bet {bet_id} is not actionable on Smarkets")
    if recommendation.get("action") != "cash_out":
        raise ValueError(f"Tracked bet {bet_id} is not currently marked for cash out")

    runtime = snapshot.get("runtime") or {}
    session = runtime.get("session") or {}
    if not bool(session.get("open_positions_ready")):
        raise ValueError(
            "Smarkets session is not on an actionable Smarkets open positions page"
        )

    detail = f"Cash out requested for {bet_id}, but the Smarkets execution contract is not implemented yet."
    updated = dict(snapshot)
    updated["worker"] = {
        **dict(snapshot.get("worker") or {}),
        "status": "error",
        "detail": detail,
    }
    updated["status_line"] = detail
    return updated
