from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import json


def build_runtime_summary(
    *,
    run_dir: Path | None,
    positions_payload: dict,
    watcher_state: dict | None = None,
) -> dict:
    runtime = {
        "updated_at": str(positions_payload.get("captured_at", "")),
        "source": "positions_snapshot",
        "refresh_kind": "live_capture",
        "decision_count": 0,
        "watcher_iteration": None,
        "stale": False,
        "session": build_runtime_session_summary(
            positions_payload=positions_payload,
            watcher_state=watcher_state,
        ),
    }
    if run_dir is None:
        return runtime

    watcher_state_path = run_dir / "watcher-state.json"
    if watcher_state is None and not watcher_state_path.exists():
        return runtime

    effective_watcher_state = watcher_state
    if effective_watcher_state is None:
        effective_watcher_state = json.loads(watcher_state_path.read_text())

    runtime["updated_at"] = str(
        effective_watcher_state.get("updated_at") or runtime["updated_at"]
    )
    runtime["source"] = "watcher-state"
    runtime["decision_count"] = int(
        effective_watcher_state.get(
            "decision_count",
            len(effective_watcher_state.get("decisions", [])),
        ),
    )
    runtime["watcher_iteration"] = effective_watcher_state.get("iteration")
    runtime["session"] = build_runtime_session_summary(
        positions_payload=positions_payload,
        watcher_state=effective_watcher_state,
    )

    interval_seconds = float(effective_watcher_state.get("interval_seconds", 0) or 0)
    if interval_seconds > 0 and runtime["updated_at"]:
        updated_at = datetime.fromisoformat(
            runtime["updated_at"].replace("Z", "+00:00")
        )
        runtime["stale"] = (datetime.now(UTC) - updated_at).total_seconds() > max(
            interval_seconds * 3,
            interval_seconds + 5,
        )
    return runtime


def build_runtime_session_summary(
    *, positions_payload: dict, watcher_state: dict | None
) -> dict | None:
    if watcher_state is not None and isinstance(watcher_state.get("session"), dict):
        return watcher_state["session"]

    current_url = str(positions_payload.get("url", "") or "")
    document_title = str(positions_payload.get("document_title", "") or "").strip()
    if not current_url and not document_title:
        return None

    return {
        "name": None,
        "current_url": current_url,
        "document_title": document_title,
        "page_hint": (
            "open_positions"
            if "open-positions" in current_url
            or "open positions" in document_title.lower()
            else "unknown"
        ),
        "open_positions_ready": (
            "open-positions" in current_url
            or "open positions" in document_title.lower()
        ),
        "validation_error": None,
    }


def load_decisions(*, run_dir: Path | None) -> list[dict]:
    if run_dir is None:
        return []

    watcher_state_path = run_dir / "watcher-state.json"
    if not watcher_state_path.exists():
        return []

    watcher_state = json.loads(watcher_state_path.read_text())
    decisions = watcher_state.get("decisions", [])
    if not isinstance(decisions, list):
        return []

    return [
        {
            "contract": str(decision.get("contract", "")),
            "market": str(decision.get("market", "")),
            "status": str(decision.get("status", "hold")),
            "reason": str(decision.get("reason", "unknown")),
            "current_pnl_amount": float(decision.get("current_pnl_amount", 0.0)),
            "current_back_odds": (
                float(decision["current_back_odds"])
                if decision.get("current_back_odds") is not None
                else None
            ),
            "profit_take_back_odds": float(decision.get("profit_take_back_odds", 0.0)),
            "stop_loss_back_odds": float(decision.get("stop_loss_back_odds", 0.0)),
        }
        for decision in decisions
    ]
