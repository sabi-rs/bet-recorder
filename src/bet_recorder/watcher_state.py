from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import json


def build_watcher_state(
  *,
  source: str,
  run_dir: Path,
  interval_seconds: float,
  iteration: int,
  snapshot: dict,
  captured_at: datetime,
) -> dict:
  watch = snapshot.get("watch") or {}
  decisions = [
    _build_decision(
      watch=row,
      target_profit=float(watch.get("target_profit", 0.0)),
      stop_loss=float(watch.get("stop_loss", 0.0)),
    )
    for row in watch.get("watches", [])
  ]
  updated_at = captured_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
  return {
    "source": source,
    "run_dir": str(run_dir),
    "updated_at": updated_at,
    "interval_seconds": interval_seconds,
    "iteration": iteration,
    "worker": snapshot.get("worker"),
    "account_stats": snapshot.get("account_stats"),
    "open_positions": snapshot.get("open_positions", []),
    "other_open_bets": snapshot.get("other_open_bets", []),
    "watch": snapshot.get("watch"),
    "decision_count": len(decisions),
    "decisions": decisions,
  }


def write_watcher_state(output_path: Path, state: dict) -> None:
  output_path.write_text(json.dumps(state, indent=2) + "\n")


def _build_decision(*, watch: dict, target_profit: float, stop_loss: float) -> dict:
  current_pnl = float(watch["current_pnl_amount"])
  if not bool(watch.get("can_trade_out")):
    status = "monitor_only"
  elif current_pnl >= target_profit:
    status = "take_profit_ready"
  elif current_pnl <= -stop_loss:
    status = "stop_loss_ready"
  else:
    status = "hold"

  return {
    "contract": watch["contract"],
    "market": watch["market"],
    "status": status,
    "current_pnl_amount": current_pnl,
    "profit_take_back_odds": float(watch["profit_take_back_odds"]),
    "stop_loss_back_odds": float(watch["stop_loss_back_odds"]),
  }
