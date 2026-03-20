from __future__ import annotations

from pathlib import Path

from bet_recorder.capture.operator_interaction import append_operator_interaction_event
from bet_recorder.capture.run_bundle import load_run_bundle
from bet_recorder.transport.writer import append_transport_marker


def handle_cash_out_tracked_bet(
    *,
    snapshot: dict,
    bet_id: str,
    run_dir: Path | None = None,
) -> dict:
    _record_cash_out_marker(
        run_dir=run_dir,
        bet_id=bet_id,
        phase="request",
        status="requested",
        detail=f"Cash out requested for {bet_id}.",
    )

    try:
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

        detail = (
            f"Cash out requested for {bet_id}, but the Smarkets execution contract is "
            "not implemented yet."
        )
        updated = dict(snapshot)
        updated["worker"] = {
            **dict(snapshot.get("worker") or {}),
            "status": "error",
            "detail": detail,
        }
        updated["status_line"] = detail
        _record_cash_out_marker(
            run_dir=run_dir,
            bet_id=bet_id,
            phase="response",
            status="not_implemented",
            detail=detail,
        )
        return updated
    except Exception as exc:
        _record_cash_out_marker(
            run_dir=run_dir,
            bet_id=bet_id,
            phase="response",
            status="error",
            detail=str(exc),
        )
        raise


def _record_cash_out_marker(
    *,
    run_dir: Path | None,
    bet_id: str,
    phase: str,
    status: str,
    detail: str,
) -> None:
    if run_dir is None:
        return
    bundle = load_run_bundle(source="smarkets_exchange", run_dir=run_dir)
    bundle.events_path.parent.mkdir(parents=True, exist_ok=True)
    bundle.events_path.touch(exist_ok=True)
    append_operator_interaction_event(
        bundle.events_path,
        action="cash_out",
        status=f"{phase}:{status}",
        detail=detail,
        reference_id=bet_id,
        metadata={"bet_id": bet_id},
    )
    if bundle.transport_path is not None:
        append_transport_marker(
            bundle.transport_path,
            action="cash_out",
            phase=phase,
            detail=detail,
            reference_id=bet_id,
            metadata={"bet_id": bet_id, "status": status},
        )
